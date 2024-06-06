//===----------------------------------------------------------------------===//
//                         DuckDB
//
// substrait-extension.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#include "duckdb/optimizer/timer_util.h"

namespace duckdb {

struct ToSubstraitFunctionData : public TableFunctionData {
    ToSubstraitFunctionData() {
    }
    string query;
    bool enable_optimizer;
    bool finished = false;
};

class SubstraitExtension : public Extension {
public:
	void Load(DuckDB &db) override;
	std::string Name() override;
};

shared_ptr<Relation> SubstraitPlanToDuckDBRel(Connection &conn, const string &serialized, bool json);

} // namespace duckdb
