#define DUCKDB_EXTENSION_MAIN

#include "from_substrait.hpp"
#include "substrait_extension.hpp"
#include "to_substrait.hpp"
#include "google/protobuf/util/json_util.h"

#ifndef DUCKDB_AMALGAMATION
#include "duckdb/common/enums/optimizer_type.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/parser/parsed_data/create_pragma_function_info.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/relation/projection_relation.hpp"
#include "duckdb/main/relation/join_relation.hpp"
#endif

namespace duckdb {

struct ToSubstraitFunctionData : public TableFunctionData {
	ToSubstraitFunctionData() {
	}
	string query;
	bool enable_optimizer;
	bool finished = false;
};

static void ToJsonFunctionInternal(ClientContext &context, ToSubstraitFunctionData &data, DataChunk &output,
                                   Connection &new_conn, unique_ptr<LogicalOperator> &query_plan, string &serialized);
static void ToSubFunctionInternal(ClientContext &context, ToSubstraitFunctionData &data, DataChunk &output,
                                  Connection &new_conn, unique_ptr<LogicalOperator> &query_plan, string &serialized);

static void VerifyJSONRoundtrip(unique_ptr<LogicalOperator> &query_plan, Connection &con, ToSubstraitFunctionData &data,
                                const string &serialized);
static void VerifyBlobRoundtrip(unique_ptr<LogicalOperator> &query_plan, Connection &con, ToSubstraitFunctionData &data,
                                const string &serialized);

static bool SetOptimizationOption(const ClientConfig &config, const duckdb::named_parameter_map_t &named_params) {
	for (const auto &param : named_params) {
		auto loption = StringUtil::Lower(param.first);
		// If the user has explicitly requested to enable/disable the optimizer when
		// generating Substrait, then that takes precedence.
		if (loption == "enable_optimizer") {
			return BooleanValue::Get(param.second);
		}
	}

	// If the user has not specified what they want, fall back to the settings
	// on the connection (e.g. if the optimizer was disabled by the user at
	// the connection level, it would be surprising to enable the optimizer
	// when generating Substrait).
	return config.enable_optimizer;
}

static unique_ptr<ToSubstraitFunctionData> InitToSubstraitFunctionData(const ClientConfig &config,
                                                                       TableFunctionBindInput &input) {
	auto result = make_uniq<ToSubstraitFunctionData>();
	result->query = input.inputs[0].ToString();
	result->enable_optimizer = SetOptimizationOption(config, input.named_parameters);
	return std::move(result);
}

static unique_ptr<FunctionData> ToSubstraitBind(ClientContext &context, TableFunctionBindInput &input,
                                                vector<LogicalType> &return_types, vector<string> &names) {
	return_types.emplace_back(LogicalType::BLOB);
	names.emplace_back("Plan Blob");
	auto result = InitToSubstraitFunctionData(context.config, input);
	return std::move(result);
}

static unique_ptr<FunctionData> ToJsonBind(ClientContext &context, TableFunctionBindInput &input,
                                           vector<LogicalType> &return_types, vector<string> &names) {
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("Json");
	auto result = InitToSubstraitFunctionData(context.config, input);
	return std::move(result);
}

shared_ptr<Relation> SubstraitPlanToDuckDBRel(Connection &conn, const string &serialized, bool json = false) {
	SubstraitToDuckDB transformer_s2d(conn, serialized, json);
	return transformer_s2d.TransformPlan();
}

static void VerifySubstraitRoundtrip(unique_ptr<LogicalOperator> &query_plan, Connection &con,
                                     ToSubstraitFunctionData &data, const string &serialized, bool is_json) {
	// We round-trip the generated json and verify if the result is the same
	auto actual_result = con.Query(data.query);

	auto sub_relation = SubstraitPlanToDuckDBRel(con, serialized, is_json);
	auto substrait_result = sub_relation->Execute();
	substrait_result->names = actual_result->names;
	unique_ptr<MaterializedQueryResult> substrait_materialized;

	if (substrait_result->type == QueryResultType::STREAM_RESULT) {
		auto &stream_query = substrait_result->Cast<duckdb::StreamQueryResult>();

		substrait_materialized = stream_query.Materialize();
	} else if (substrait_result->type == QueryResultType::MATERIALIZED_RESULT) {
		substrait_materialized = unique_ptr_cast<QueryResult, MaterializedQueryResult>(std::move(substrait_result));
	}
	auto actual_col_coll = actual_result->Collection();
	auto subs_col_coll = substrait_materialized->Collection();
	string error_message;
	if (!ColumnDataCollection::ResultEquals(actual_col_coll, subs_col_coll, error_message)) {
		query_plan->Print();
		sub_relation->Print();
		throw InternalException("The query result of DuckDB's query plan does not match Substrait : " + error_message);
	}
}

static void VerifyBlobRoundtrip(unique_ptr<LogicalOperator> &query_plan, Connection &con, ToSubstraitFunctionData &data,
                                const string &serialized) {
	VerifySubstraitRoundtrip(query_plan, con, data, serialized, false);
}

static void VerifyJSONRoundtrip(unique_ptr<LogicalOperator> &query_plan, Connection &con, ToSubstraitFunctionData &data,
                                const string &serialized) {
	VerifySubstraitRoundtrip(query_plan, con, data, serialized, true);
}

static DuckDBToSubstrait InitPlanExtractor(ClientContext &context, ToSubstraitFunctionData &data, Connection &new_conn,
                                           unique_ptr<LogicalOperator> &query_plan) {
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
	DBConfig::GetConfig(*new_conn.context).options.disabled_optimizers = disabled_optimizers;

	query_plan = new_conn.context->ExtractPlan(data.query);
#if ENABLE_DEBUG_PRINT
    Printer::Print("optimized logical plan");
    query_plan->Print();
#endif
	return DuckDBToSubstrait(context, *query_plan);
}

static void ToSubFunctionInternal(ClientContext &context, ToSubstraitFunctionData &data, DataChunk &output,
                                  Connection &new_conn, unique_ptr<LogicalOperator> &query_plan, string &serialized) {
	output.SetCardinality(1);
	auto transformer_d2s = InitPlanExtractor(context, data, new_conn, query_plan);
	serialized = transformer_d2s.SerializeToString();
	output.SetValue(0, 0, Value::BLOB_RAW(serialized));
}

static void ToSubFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = (ToSubstraitFunctionData &)*data_p.bind_data;
	if (data.finished) {
		return;
	}
	auto new_conn = Connection(*context.db);

	unique_ptr<LogicalOperator> query_plan;
	string serialized;
	ToSubFunctionInternal(context, data, output, new_conn, query_plan, serialized);

	data.finished = true;

	if (!context.config.query_verification_enabled) {
		return;
	}
	VerifyBlobRoundtrip(query_plan, new_conn, data, serialized);
	// Also run the ToJson path and verify round-trip for that
	DataChunk other_output;
	other_output.Initialize(context, {LogicalType::VARCHAR});
	ToJsonFunctionInternal(context, data, other_output, new_conn, query_plan, serialized);
	VerifyJSONRoundtrip(query_plan, new_conn, data, serialized);
}

static void ToJsonFunctionInternal(ClientContext &context, ToSubstraitFunctionData &data, DataChunk &output,
                                   Connection &new_conn, unique_ptr<LogicalOperator> &query_plan, string &serialized) {
	output.SetCardinality(1);
	auto transformer_d2s = InitPlanExtractor(context, data, new_conn, query_plan);
	serialized = transformer_d2s.SerializeToJson();
	output.SetValue(0, 0, serialized);
}

static void ToJsonFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = (ToSubstraitFunctionData &)*data_p.bind_data;
	if (data.finished) {
		return;
	}
	auto new_conn = Connection(*context.db);

	unique_ptr<LogicalOperator> query_plan;
	string serialized;
	ToJsonFunctionInternal(context, data, output, new_conn, query_plan, serialized);

	data.finished = true;

	if (!context.config.query_verification_enabled) {
		return;
	}
	VerifyJSONRoundtrip(query_plan, new_conn, data, serialized);
	// Also run the ToJson path and verify round-trip for that
	DataChunk other_output;
	other_output.Initialize(context, {LogicalType::BLOB});
	ToSubFunctionInternal(context, data, other_output, new_conn, query_plan, serialized);
	VerifyBlobRoundtrip(query_plan, new_conn, data, serialized);
}

struct FromSubstraitFunctionData : public TableFunctionData {
	FromSubstraitFunctionData() = default;
	shared_ptr<Relation> plan;
	unique_ptr<QueryResult> res;
	unique_ptr<Connection> conn;
};

static unique_ptr<FunctionData> SubstraitBind(ClientContext &context, TableFunctionBindInput &input,
                                              vector<LogicalType> &return_types, vector<string> &names, bool is_json) {
	auto result = make_uniq<FromSubstraitFunctionData>();
	result->conn = make_uniq<Connection>(*context.db);
	if (input.inputs[0].IsNull()) {
		throw BinderException("from_substrait cannot be called with a NULL parameter");
	}
	string serialized = input.inputs[0].GetValueUnsafe<string>();
	result->plan = SubstraitPlanToDuckDBRel(*result->conn, serialized, is_json);
	for (auto &column : result->plan->Columns()) {
		return_types.emplace_back(column.Type());
		names.emplace_back(column.Name());
	}
	return std::move(result);
}

static unique_ptr<FunctionData> FromSubstraitBind(ClientContext &context, TableFunctionBindInput &input,
                                                  vector<LogicalType> &return_types, vector<string> &names) {
	return SubstraitBind(context, input, return_types, names, false);
}

static unique_ptr<FunctionData> FromSubstraitBindJSON(ClientContext &context, TableFunctionBindInput &input,
                                                      vector<LogicalType> &return_types, vector<string> &names) {
	return SubstraitBind(context, input, return_types, names, true);
}

static void FromSubFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = (FromSubstraitFunctionData &)*data_p.bind_data;
    timespec timer = tic();
	if (!data.res) {
		data.res = data.plan->Execute();
	}
	auto result_chunk = data.res->Fetch();
	if (!result_chunk) {
		return;
	}
    toc(&timer, "Duckdb::Relation execution time is\n");
	output.Move(*result_chunk);
}

void InitializeGetSubstrait(Connection &con) {
	auto &catalog = Catalog::GetSystemCatalog(*con.context);

	// create the get_substrait table function that allows us to get a substrait
	// binary from a valid SQL Query
	TableFunction to_sub_func("get_substrait", {LogicalType::VARCHAR}, ToSubFunction, ToSubstraitBind);
	to_sub_func.named_parameters["enable_optimizer"] = LogicalType::BOOLEAN;
	CreateTableFunctionInfo to_sub_info(to_sub_func);
	catalog.CreateTableFunction(*con.context, to_sub_info);
}

void InitializeGetSubstraitJSON(Connection &con) {
	auto &catalog = Catalog::GetSystemCatalog(*con.context);

	// create the get_substrait table function that allows us to get a substrait
	// JSON from a valid SQL Query
	TableFunction get_substrait_json("get_substrait_json", {LogicalType::VARCHAR}, ToJsonFunction, ToJsonBind);

	get_substrait_json.named_parameters["enable_optimizer"] = LogicalType::BOOLEAN;
	CreateTableFunctionInfo get_substrait_json_info(get_substrait_json);
	catalog.CreateTableFunction(*con.context, get_substrait_json_info);
}

void InitializeFromSubstrait(Connection &con) {
	auto &catalog = Catalog::GetSystemCatalog(*con.context);

	// create the from_substrait table function that allows us to get a query
	// result from a substrait plan
	TableFunction from_sub_func("from_substrait", {LogicalType::BLOB}, FromSubFunction, FromSubstraitBind);
	CreateTableFunctionInfo from_sub_info(from_sub_func);
	catalog.CreateTableFunction(*con.context, from_sub_info);
}

void InitializeFromSubstraitJSON(Connection &con) {
	auto &catalog = Catalog::GetSystemCatalog(*con.context);

	// create the from_substrait table function that allows us to get a query
	// result from a substrait plan
	TableFunction from_sub_func_json("from_substrait_json", {LogicalType::VARCHAR}, FromSubFunction,
	                                 FromSubstraitBindJSON);
	CreateTableFunctionInfo from_sub_info_json(from_sub_func_json);
	catalog.CreateTableFunction(*con.context, from_sub_info_json);
}

bool GetSubQueries(substrait::Rel *plan_rel, std::queue<substrait::Rel > &subquery_queue) {
    bool can_split = false;
    switch (plan_rel->rel_type_case()) {
        case substrait::Rel::RelTypeCase::kJoin:
            GetSubQueries(plan_rel->mutable_join()->mutable_left(), subquery_queue);
            GetSubQueries(plan_rel->mutable_join()->mutable_right(), subquery_queue);
            can_split = true;
            break;
        case substrait::Rel::RelTypeCase::kCross:
            GetSubQueries(plan_rel->mutable_cross()->mutable_left(), subquery_queue);
            GetSubQueries(plan_rel->mutable_cross()->mutable_right(), subquery_queue);
            break;
        case substrait::Rel::RelTypeCase::kFetch:
            GetSubQueries(plan_rel->mutable_fetch()->mutable_input(), subquery_queue);
            break;
        case substrait::Rel::RelTypeCase::kFilter:
            GetSubQueries(plan_rel->mutable_filter()->mutable_input(), subquery_queue);
            break;
        case substrait::Rel::RelTypeCase::kProject:
            if (GetSubQueries(plan_rel->mutable_project()->mutable_input(), subquery_queue)) {
                plan_rel->mutable_project()->set_split_point();
                subquery_queue.emplace(*plan_rel);
            }
            break;
        case substrait::Rel::RelTypeCase::kAggregate:
            GetSubQueries(plan_rel->mutable_aggregate()->mutable_input(), subquery_queue);
            break;
        case substrait::Rel::RelTypeCase::kRead:
            break;
        case substrait::Rel::RelTypeCase::kSort:
            GetSubQueries(plan_rel->mutable_sort()->mutable_input(), subquery_queue);
            break;
        case substrait::Rel::RelTypeCase::kSet:
            // todo: fix when meet
            GetSubQueries(plan_rel->mutable_set()->mutable_inputs(0), subquery_queue);
            break;
        default:
            throw InternalException("Unsupported relation type " + to_string(plan_rel->rel_type_case()));
    }
    return can_split;
}

bool GetMergedPlan(substrait::Rel *plan_rel, substrait::ReadRel *temp_table) {
    switch (plan_rel->rel_type_case()) {
        case substrait::Rel::RelTypeCase::kJoin:
            GetMergedPlan(plan_rel->mutable_join()->mutable_left(), temp_table);
            GetMergedPlan(plan_rel->mutable_join()->mutable_right(), temp_table);
            break;
        case substrait::Rel::RelTypeCase::kCross:
            GetMergedPlan(plan_rel->mutable_cross()->mutable_left(), temp_table);
            GetMergedPlan(plan_rel->mutable_cross()->mutable_right(), temp_table);
            break;
        case substrait::Rel::RelTypeCase::kFetch:
            GetMergedPlan(plan_rel->mutable_fetch()->mutable_input(), temp_table);
            break;
        case substrait::Rel::RelTypeCase::kFilter:
            GetMergedPlan(plan_rel->mutable_filter()->mutable_input(), temp_table);
            break;
        case substrait::Rel::RelTypeCase::kProject:
            if (plan_rel->mutable_project()->split_point) {
                plan_rel->clear_project();
                // merge with the temp table
                plan_rel->mutable_read()->CopyFrom(*temp_table);
            } else {
                GetMergedPlan(plan_rel->mutable_project()->mutable_input(), temp_table);
            }
            break;
        case substrait::Rel::RelTypeCase::kAggregate:
            GetMergedPlan(plan_rel->mutable_aggregate()->mutable_input(), temp_table);
            break;
        case substrait::Rel::RelTypeCase::kRead:
            break;
        case substrait::Rel::RelTypeCase::kSort:
            GetMergedPlan(plan_rel->mutable_sort()->mutable_input(), temp_table);
            break;
        case substrait::Rel::RelTypeCase::kSet:
            // todo: fix when meet
            GetMergedPlan(plan_rel->mutable_set()->mutable_inputs(0), temp_table);
            break;
        default:
            throw InternalException("Unsupported relation type " + to_string(plan_rel->rel_type_case()));
    }
    return false;
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

unique_ptr<QueryResult> PlanTest(ClientContext &context, substrait::Plan plan, Connection &new_conn) {
    std::queue<substrait::Rel > subquery_queue;
    GetSubQueries(plan.mutable_relations(0)->mutable_root()->mutable_input(), subquery_queue);

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
        subquery_plan.mutable_relations(0)->mutable_root()->mutable_input()->mutable_project()->mutable_input()
            ->CopyFrom(subquery_queue.front());
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

        subquery_queue.pop();
        if (subquery_queue.empty()) {
            substrait_result = sub_relation->Execute();
            break;
        }

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

        GetMergedPlan(subquery_queue.front().mutable_project()->mutable_input(), temp_table_substrait_plan.mutable_relations(0)->mutable_root()
            ->mutable_input()->mutable_project()->mutable_input()->mutable_read());

        // todo: update index after merge if we only select the necessary columns
//        subquery_queue.front().mutable_project()->mutable_expressions(3)->mutable_selection()
//            ->mutable_direct_reference()->mutable_struct_field()->set_field(3);

#if ENABLE_DEBUG_PRINT
        Printer::Print("after GetMergedPlan");
        Printer::Print(subquery_queue.front().DebugString());
#endif
    }

    subquery_plan.Clear();
    plan.Clear();
    temp_table_substrait_plan.Clear();

    return substrait_result;
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

static void QuerySplit(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
    // from `ToJsonFunction`
    auto &data = (ToSubstraitFunctionData &)*data_p.bind_data;
	if (data.finished) {
		return;
	}
	auto new_conn = Connection(*context.db);

	unique_ptr<LogicalOperator> query_plan;
    auto transformer_d2s = InitPlanExtractor(context, data, new_conn, query_plan);
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

shared_ptr<Relation> SubstraitPlanToDuckDBRel(Connection &conn, const substrait::Plan &substrait_plan) {
    SubstraitToDuckDB transformer_s2d(conn, substrait_plan);
    return transformer_s2d.TransformPlan();
}

static void EndToEndFunctionInternal(ClientContext &context, ToSubstraitFunctionData &data, DataChunk &output,
                                   Connection &new_conn, unique_ptr<LogicalOperator> &query_plan) {
    auto transformer_d2s = InitPlanExtractor(context, data, new_conn, query_plan);
    auto substrait_plan = transformer_d2s.GetPlan();

#if ENABLE_DEBUG_PRINT
        Printer::Print("substrait_plan");
        Printer::Print(substrait_plan.DebugString());
#endif

    auto duckdb_rel = SubstraitPlanToDuckDBRel(new_conn, substrait_plan);
#if ENABLE_DEBUG_PRINT
    Printer::Print("duckdb_rel");
    duckdb_rel->Print();
#endif

    timespec timer = tic();
    auto res = duckdb_rel->Execute();
    toc(&timer, "Duckdb::Relation execution time is\n");

    // debug: print the result
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
    Printer::Print("result: ");
    collection->Print();

    // todo: uncomment once setting `return_types` and `names` in `EndToEndBind`.
//    auto result_chunk = res->Fetch();
//    if (!result_chunk) {
//        Printer::Print("Error: result is empty!");
//        return;
//    }
//    output.Move(*result_chunk);
}

static void EndToEndFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
    auto &data = (ToSubstraitFunctionData &)*data_p.bind_data;
    if (data.finished) {
        return;
    }
    auto new_conn = Connection(*context.db);

    unique_ptr<LogicalOperator> query_plan;
    EndToEndFunctionInternal(context, data, output, new_conn, query_plan);

    data.finished = true;
}

static unique_ptr<FunctionData> EndToEndBind(ClientContext &context, TableFunctionBindInput &input,
                                           vector<LogicalType> &return_types, vector<string> &names) {
    // todo: get the duckdb_rel after converting the substrait::Plan to relation
//    for (auto &column : duckdb_rel->Columns()) {
//        return_types.emplace_back(column.Type());
//        names.emplace_back(column.Name());
//    }

    return_types.emplace_back(LogicalType::VARCHAR);
    names.emplace_back("Json");
    auto result = InitToSubstraitFunctionData(context.config, input);
    return std::move(result);
}

void InitializeEndToEnd(Connection &con) {
    auto &catalog = Catalog::GetSystemCatalog(*con.context);

    // the end to end function combining with "InitializeGetSubstraitJSON" and "InitializeFromSubstraitJSON"
    TableFunction end_to_end("end_to_end", {LogicalType::VARCHAR}, EndToEndFunction, EndToEndBind);

    end_to_end.named_parameters["enable_optimizer"] = LogicalType::BOOLEAN;
    CreateTableFunctionInfo end_to_end_info(end_to_end);
    catalog.CreateTableFunction(*con.context, end_to_end_info);
}

void InitializeQuerySplitEndToEnd(Connection &con) {
    auto &catalog = Catalog::GetSystemCatalog(*con.context);

    // the end to end function combining with "InitializeGetSubstraitJSON" and "InitializeFromSubstraitJSON"
    TableFunction end_to_end("query_split", {LogicalType::VARCHAR}, QuerySplit, EndToEndBind);

    end_to_end.named_parameters["enable_optimizer"] = LogicalType::BOOLEAN;
    CreateTableFunctionInfo end_to_end_info(end_to_end);
    catalog.CreateTableFunction(*con.context, end_to_end_info);
}

void SubstraitExtension::Load(DuckDB &db) {
	Connection con(db);
	con.BeginTransaction();

	InitializeGetSubstrait(con);
	InitializeGetSubstraitJSON(con);

	InitializeFromSubstrait(con);
	InitializeFromSubstraitJSON(con);

    InitializeEndToEnd(con);

    InitializeQuerySplitEndToEnd(con);

	con.Commit();
}

std::string SubstraitExtension::Name() {
	return "substrait";
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void substrait_init(duckdb::DatabaseInstance &db) {
	duckdb::DuckDB db_wrapper(db);
	db_wrapper.LoadExtension<duckdb::SubstraitExtension>();
}

DUCKDB_EXTENSION_API const char *substrait_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}
