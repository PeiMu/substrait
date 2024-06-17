//===----------------------------------------------------------------------===//
//                         DuckDB
//
// substrait-extension.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <queue>
#include "substrait_extension.hpp"

#ifndef DUCKDB_AMALGAMATION
#include "duckdb.hpp"
#include "duckdb/optimizer/timer_util.h"
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

#define ENABLE_DEBUG_PRINT true

namespace duckdb {

struct substrait_struct {
    substrait::Rel subquery;
    int split_index;
};

using subquery_queue = std::queue<std::vector<substrait_struct>>;

void QuerySplit(ClientContext &context, TableFunctionInput &data_p, DataChunk &output);

} // namespace duckdb