#define DUCKDB_EXTENSION_MAIN

#include "from_substrait.hpp"
#include "to_substrait.hpp"
#include "google/protobuf/util/json_util.h"

#include "query_split/query_split.h"

namespace duckdb {

static DuckDBToSubstrait QuerySplitPlanExtractor(ClientContext &context, ToSubstraitFunctionData &data,
                                                 Connection &new_conn, unique_ptr<LogicalOperator> &query_plan) {
    // The user might want to disable the optimizer of the new connection
    new_conn.context->config.enable_optimizer = data.enable_optimizer;
    new_conn.context->config.use_replacement_scans = false;

    // We want for sure to disable the internal compression optimizations.
    // These are DuckDB specific, no other system implements these. Also,
    // respect the user's settings if they chose to disable any specific optimizers.
    //
    // The InClauseRewriter optimization converts large `IN` clauses to a
    // "mark join" against a `ColumnDataCollection`, which may not make
    // sense in other systems and would complicate the conversion to Substrait.
    set<OptimizerType> disabled_optimizers = DBConfig::GetConfig(context).options.disabled_optimizers;
    disabled_optimizers.insert(OptimizerType::IN_CLAUSE);
    disabled_optimizers.insert(OptimizerType::COMPRESSED_MATERIALIZATION);
    // todo: only have the pre optimizations before query split, then have the post optimizations
    DBConfig::GetConfig(*new_conn.context).options.disabled_optimizers = disabled_optimizers;

    query_plan = new_conn.context->ExtractPlan(data.query);
#if ENABLE_DEBUG_PRINT
    Printer::Print("optimized logical plan");
    query_plan->Print();
#endif
    return DuckDBToSubstrait(context, *query_plan);
}

bool GetSubQueries(substrait::Rel *plan_rel, subquery_queue &subquery_queue,
                   std::vector<substrait_struct> &current_level_subquery,
                   int &split_index) {
    bool can_split = false;
    switch (plan_rel->rel_type_case()) {
        case substrait::Rel::RelTypeCase::kJoin: {
            std::vector<substrait_struct> current_join_subquery;
            GetSubQueries(plan_rel->mutable_join()->mutable_left(), subquery_queue, current_join_subquery, split_index);
            GetSubQueries(plan_rel->mutable_join()->mutable_right(), subquery_queue, current_join_subquery, split_index);
            can_split = true;
            if (!current_join_subquery.empty()) {
                subquery_queue.emplace(current_join_subquery);
                current_join_subquery.clear();
                current_level_subquery.clear();
            }
            // we only insert if we get `current_level_subquery` (`current_join_subquery` here) from proj
            return can_split;
        }
        case substrait::Rel::RelTypeCase::kProject:
            if (GetSubQueries(plan_rel->mutable_project()->mutable_input(), subquery_queue, current_level_subquery,
                              split_index)) {
                plan_rel->mutable_project()->set_split_point(split_index);
                substrait_struct subquery_struct{*plan_rel, split_index};
                std::string debug_info = "Split point: " + std::to_string(split_index) + ": " + plan_rel->DebugString();
                Printer::Print(debug_info);
                split_index++;
                current_level_subquery.emplace_back(subquery_struct);
            }
            // we only set split point here, and pass the `current_level_subquery` to the upper level
            // do not actually insert plan_rel to the queue
            return can_split;
        case substrait::Rel::RelTypeCase::kCross:
            GetSubQueries(plan_rel->mutable_cross()->mutable_left(), subquery_queue, current_level_subquery, split_index);
            GetSubQueries(plan_rel->mutable_cross()->mutable_right(), subquery_queue, current_level_subquery, split_index);
            break;
        case substrait::Rel::RelTypeCase::kFetch:
            GetSubQueries(plan_rel->mutable_fetch()->mutable_input(), subquery_queue, current_level_subquery, split_index);
            break;
        case substrait::Rel::RelTypeCase::kFilter:
            GetSubQueries(plan_rel->mutable_filter()->mutable_input(), subquery_queue, current_level_subquery, split_index);
            break;
        case substrait::Rel::RelTypeCase::kAggregate:
            GetSubQueries(plan_rel->mutable_aggregate()->mutable_input(), subquery_queue, current_level_subquery, split_index);
            break;
        case substrait::Rel::RelTypeCase::kRead:
            break;
        case substrait::Rel::RelTypeCase::kSort:
            GetSubQueries(plan_rel->mutable_sort()->mutable_input(), subquery_queue, current_level_subquery, split_index);
            break;
        case substrait::Rel::RelTypeCase::kSet:
            // todo: fix when meet
            GetSubQueries(plan_rel->mutable_set()->mutable_inputs(0), subquery_queue, current_level_subquery, split_index);
            break;
        default:
            throw InternalException("Unsupported relation type " + to_string(plan_rel->rel_type_case()));
    }

    // insert to the queue for the rest cases
    if (!current_level_subquery.empty()) {
        subquery_queue.emplace(current_level_subquery);
        current_level_subquery.clear();
    }

    return can_split;
}

bool GetMergedPlan(substrait::Rel *plan_rel, substrait::ReadRel *temp_table, int merge_index) {
    bool merged_success = false;
    switch (plan_rel->rel_type_case()) {
        case substrait::Rel::RelTypeCase::kJoin:
        {
            bool left_merged_success = GetMergedPlan(plan_rel->mutable_join()->mutable_left(), temp_table, merge_index);
            bool right_merged_success = GetMergedPlan(plan_rel->mutable_join()->mutable_right(), temp_table, merge_index);
            merged_success = left_merged_success | right_merged_success;
            break;
        }
        case substrait::Rel::RelTypeCase::kCross:
        {
            bool left_merged_success = GetMergedPlan(plan_rel->mutable_cross()->mutable_left(), temp_table, merge_index);
            bool right_merged_success = GetMergedPlan(plan_rel->mutable_cross()->mutable_right(), temp_table, merge_index);
            merged_success = left_merged_success | right_merged_success;
            break;
        }
        case substrait::Rel::RelTypeCase::kFetch:
            merged_success = GetMergedPlan(plan_rel->mutable_fetch()->mutable_input(), temp_table, merge_index);
            break;
        case substrait::Rel::RelTypeCase::kFilter:
            merged_success = GetMergedPlan(plan_rel->mutable_filter()->mutable_input(), temp_table, merge_index);
            break;
        case substrait::Rel::RelTypeCase::kProject:
            if (plan_rel->mutable_project()->split_point == merge_index) {
                plan_rel->clear_project();
                // merge with the temp table
                plan_rel->mutable_read()->CopyFrom(*temp_table);
                std::string debug_info = "Merge point: " + std::to_string(merge_index) + ": " + plan_rel->DebugString();
                Printer::Print(debug_info);
                merged_success = true;
            } else {
                merged_success = GetMergedPlan(plan_rel->mutable_project()->mutable_input(), temp_table, merge_index);
            }
            break;
        case substrait::Rel::RelTypeCase::kAggregate:
            merged_success = GetMergedPlan(plan_rel->mutable_aggregate()->mutable_input(), temp_table, merge_index);
            break;
        case substrait::Rel::RelTypeCase::kRead:
            break;
        case substrait::Rel::RelTypeCase::kSort:
            merged_success = GetMergedPlan(plan_rel->mutable_sort()->mutable_input(), temp_table, merge_index);
            break;
        case substrait::Rel::RelTypeCase::kSet:
            // todo: fix when meet
            merged_success = GetMergedPlan(plan_rel->mutable_set()->mutable_inputs(0), temp_table, merge_index);
            break;
        default:
            throw InternalException("Unsupported relation type " + to_string(plan_rel->rel_type_case()));
    }
    return merged_success;
}

void GetMergedPlanPair(std::vector<substrait_struct> &current_level_children, substrait::ReadRel *temp_table,
                       int merge_index) {
    for (auto & child : current_level_children) {
        if (GetMergedPlan(child.subquery.mutable_project()->mutable_input(), temp_table, merge_index)) {
            // todo: update index after merge if we only select the necessary columns
//        subquery_queue.front().mutable_project()->mutable_expressions(3)->mutable_selection()
//            ->mutable_direct_reference()->mutable_struct_field()->set_field(3);

#if ENABLE_DEBUG_PRINT
            Printer::Print("after GetMergedPlan");
            Printer::Print(child.subquery.DebugString());
#endif
        }
    }
}

// debug function
void ExecuteSQL(ClientContext &context, Connection &conn, const std::string &sql_command) {
    auto sql_query_plan = conn.context->ExtractPlan(sql_command);
    Printer::Print("sql_query_plan");
    sql_query_plan->Print();
    auto sql_json = DuckDBToSubstrait(context, *sql_query_plan).SerializeToJson();
    // debug
    Printer::Print("sql_json");
    Printer::Print(sql_json);
    auto sql_relation = SubstraitPlanToDuckDBRel(conn, sql_json, true);
    Printer::Print("sql_relation");
    sql_relation->Print();
    auto sql_result = sql_relation->Execute();
}

void RelationTest(const shared_ptr<Relation>& relation) {
    // todo: split the `relation`
    auto &proj_rel = relation->Cast<ProjectionRelation>();
    proj_rel.VisitChildren();
    auto child_rel = proj_rel.ChildRelation();
    unique_ptr<QueryResult> debug_res;
    Relation *sub_rel = relation.get();
    while (child_rel) {
        if (child_rel->type == RelationType::JOIN_RELATION) {
            auto &join_rel = child_rel->Cast<JoinRelation>();

            // debug
            auto view = join_rel.CreateView("debug_view");
            auto sub_result = view->Execute();

//            auto table_rel = join_rel.CreateRel(INVALID_SCHEMA, "debug_table");
            break;
        } else {
            sub_rel = child_rel;
            child_rel = child_rel->ChildRelation();
        }
    }

//	auto substrait_result = relation->Execute();
//    // debug
//    unique_ptr<DataChunk> result_chunk;
//    ErrorData error;
//    if (substrait_result->TryFetch(result_chunk, error)) {
//        Printer::Print("substrait_result");
//        result_chunk->Print();
//    }
//
//	unique_ptr<MaterializedQueryResult> substrait_materialized;
//
//	if (substrait_result->type == QueryResultType::STREAM_RESULT) {
//		auto &stream_query = substrait_result->Cast<duckdb::StreamQueryResult>();
//
//		substrait_materialized = stream_query.Materialize();
//	} else if (substrait_result->type == QueryResultType::MATERIALIZED_RESULT) {
//		substrait_materialized = unique_ptr_cast<QueryResult, MaterializedQueryResult>(std::move(substrait_result));
//	}
//	auto subs_col_coll = substrait_materialized->Collection();
//    // debug
//    Printer::Print("subs_col_coll");
//    subs_col_coll.Print();
//
//    // todo: merge the previous result
//    // Create(const string &table_name);
//    // CreateView(const string &name, bool replace = true, bool temporary = false);
}

unique_ptr<QueryResult> PlanTest(ClientContext &context, substrait::Plan plan, Connection &new_conn) {
#if ENABLE_DEBUG_PRINT
    Printer::Print("init plan");
    Printer::Print(plan.DebugString());
#endif
    subquery_queue subquery_queue;
    std::vector<substrait_struct> current_level_subquery;
    int split_index = 0;
    GetSubQueries(plan.mutable_relations(0)->mutable_root()->mutable_input(), subquery_queue, current_level_subquery,
                  split_index);

    substrait::Plan subquery_plan;
    subquery_plan.CopyFrom(plan);

    // todo: one potential optimization idea: we only select the necessary columns for each subquery

    // std::queue<std::vector<int32_t>> column_indexes;
    // std::queue<std::vector<std::string>> expr_names;
    // todo: 1. decide the necessary indexes and names;

    substrait::Plan temp_table_substrait_plan;
    unique_ptr<QueryResult> substrait_result;

    while (!subquery_queue.empty()) {
        // add projection head
        // add to root_rel_test
        // todo: execute in parallel
        subquery_plan.mutable_relations(0)->mutable_root()->mutable_input()->mutable_project()->mutable_input()
                ->CopyFrom(subquery_queue.front()[0].subquery);
//        // add column indexes
//        subquery_plan.mutable_relations(0)->mutable_root()->mutable_input()->mutable_project()->clear_expressions();
//        for (size_t idx = 0; idx < column_indexes.front().size(); idx++) {
//            subquery_plan.mutable_relations(0)->mutable_root()->mutable_input()->mutable_project()->add_expressions()
//                    ->mutable_selection()->mutable_direct_reference()->mutable_struct_field()->set_field(column_indexes.front()[idx]);
//            subquery_plan.mutable_relations(0)->mutable_root()->mutable_input()->mutable_project()->mutable_expressions(idx)
//                    ->mutable_selection()->mutable_root_reference();
//        }
//        column_indexes.pop();

//        // add names for the next subquery
//        subquery_plan.mutable_relations(0)->mutable_root()->clear_names();
//        for (const auto &expr_name : expr_names.front()) {
//            subquery_plan.mutable_relations(0)->mutable_root()->add_names(expr_name);
//        }

#if ENABLE_DEBUG_PRINT
        Printer::Print("subquery_plan");
        Printer::Print(subquery_plan.DebugString());
#endif

        SubstraitToDuckDB transformer_s2d(new_conn, subquery_plan);
        auto sub_relation = transformer_s2d.TransformPlan();

        // todo: execute in parallel
        int merge_index = subquery_queue.front()[0].split_index;

        subquery_queue.pop();
        if (subquery_queue.empty()) {
            substrait_result = sub_relation->Execute();
            break;
        }

        // todo: need sub-name when supporting parallel
        std::string temp_table_name = "temp_table_" + std::to_string(subquery_queue.size());
        sub_relation->Create(temp_table_name);

        std::string test_select_item;

        // option 1: only select the necessary columns
//        for (const auto &expr_name : expr_names.front()) {
//            test_select_item.append(temp_table_name).append(".").append(expr_name).append(",");
//        }
//        // delete the last comma
//        test_select_item.pop_back();
#if ENABLE_DEBUG_PRINT
//        Printer::Print("test_select_item");
//        Printer::Print(test_select_item);
#endif

        // option 2: select all from temp_table
        test_select_item = "*";

//        expr_names.pop();

        // todo: one potential optimization idea: construct the temp_table manually to speed it up
        auto test_select_temp_table = "SELECT " + test_select_item.append(" FROM ").append(temp_table_name).append(";");
#if ENABLE_DEBUG_PRINT
        Printer::Print(test_select_temp_table);
#endif

        auto temp_table_plan = new_conn.context->ExtractPlan(test_select_temp_table);

        temp_table_substrait_plan = DuckDBToSubstrait(context, *temp_table_plan).GetPlan();
#if ENABLE_DEBUG_PRINT
        Printer::Print("temp_table_substrait_plan");
        Printer::Print(temp_table_substrait_plan.DebugString());
#endif

        GetMergedPlanPair(subquery_queue.front(), temp_table_substrait_plan.mutable_relations(0)->mutable_root()
                ->mutable_input()->mutable_project()->mutable_input()->mutable_read(), merge_index);
    }

    subquery_plan.Clear();
    plan.Clear();
    temp_table_substrait_plan.Clear();

    return substrait_result;
}

void QuerySplit(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
    // from `ToJsonFunction`
    auto &data = (ToSubstraitFunctionData & ) * data_p.bind_data;
    if (data.finished) {
        return;
    }
    auto new_conn = Connection(*context.db);

    unique_ptr<LogicalOperator> query_plan;
    auto transformer_d2s = QuerySplitPlanExtractor(context, data, new_conn, query_plan);
    auto substrait_plan = transformer_d2s.GetPlan();
    // execute it
    auto res = PlanTest(context, substrait_plan, new_conn);
//    RelationTest(SubstraitPlanToDuckDBRel(new_conn, serialized, false));

    // debug
    vector<LogicalType> types = res->types;
    unique_ptr<MaterializedQueryResult> result_materialized;
    auto collection = make_uniq<ColumnDataCollection>(Allocator::DefaultAllocator(), types);
    if (res->type == QueryResultType::STREAM_RESULT) {
        auto &stream_query = res->Cast<duckdb::StreamQueryResult>();
        result_materialized = stream_query.Materialize();
        collection = make_uniq<ColumnDataCollection>(result_materialized->Collection());
    } else if (res->type == QueryResultType::MATERIALIZED_RESULT) {
        ColumnDataAppendState append_state;
        collection->InitializeAppend(append_state);
        unique_ptr<DataChunk> chunk;
        ErrorData error;
        while (true) {
            res->TryFetch(chunk, error);
            if (!chunk || chunk->size() == 0) {
                break;
            }
            // set chunk cardinality
            chunk->SetCardinality(chunk->size());
            collection->Append(append_state, *chunk);
        }
    }

    Printer::Print("query split result:");
    collection->Print();

    data.finished = true;
}

}