//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// udf_util.cpp
//
// Identification: src/udf/udf_util.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "udf/udf_util.h"
#include "codegen/buffering_consumer.h"
#include "codegen/proxy/string_functions_proxy.h"
#include "codegen/query.h"
#include "codegen/query_cache.h"
#include "codegen/query_compiler.h"
#include "codegen/type/decimal_type.h"
#include "codegen/type/integer_type.h"
#include "codegen/type/type.h"
#include "codegen/value.h"
#include "concurrency/transaction_manager_factory.h"
#include "executor/executor_context.h"
#include "executor/executors.h"
#include "optimizer/optimizer.h"
#include "parser/postgresparser.h"
#include "traffic_cop/traffic_cop.h"

namespace peloton {
namespace udf {

llvm::Type *UDFUtil::GetCodegenType(type::TypeId type_val,
                                    peloton::codegen::CodeGen &cg) {
  // TODO[Siva]: Add more types later and rename to llvm type
  switch (type_val) {
    case type::TypeId::INTEGER: {
      return cg.Int32Type();
    }
    case type::TypeId::DECIMAL: {
      return cg.DoubleType();
    }
    case type::TypeId::VARCHAR: {
      return peloton::codegen::StrWithLenProxy::GetType(cg);
    }
    default: { throw Exception("UDFHandler : Expression type not supported"); }
  }
}

void UDFUtil::ExecuteSQLHelper(const char *query, uint32_t len,
                               double *output) {
  (void)len;
  std::unique_ptr<optimizer::AbstractOptimizer> optimizer(
      new optimizer::Optimizer());
  auto &traffic_cop = tcop::TrafficCop::GetInstance();
  auto &peloton_parser = parser::PostgresParser::GetInstance();

  std::vector<ResultValue> result;
  std::vector<type::Value> params;

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  traffic_cop.SetTcopTxnState(txn);

  std::string query_string(query);

  // parse the query
  auto parsed_stmt = peloton_parser.BuildParseTree(query_string);

  // create a plan
  auto plan =
      optimizer->BuildPelotonPlanTree(parsed_stmt, DEFAULT_DB_NAME, txn);

  // perform binding
  planner::BindingContext context;
  plan->PerformBinding(context);

  // Prepare output buffer
  std::vector<oid_t> columns;
  plan->GetOutputColumns(columns);
  codegen::BufferingConsumer consumer{columns, context};

  std::unique_ptr<executor::ExecutorContext> executor_context(
      new executor::ExecutorContext(txn,
                                    codegen::QueryParameters(*plan, params)));

  // compile the query
  codegen::Query *compiled_query = codegen::QueryCache::Instance().Find(plan);
  if (compiled_query == nullptr) {
    codegen::QueryCompiler compiler;
    auto unique_compiled_query = compiler.Compile(
        *plan, executor_context->GetParams().GetQueryParametersMap(), consumer);
    compiled_query = unique_compiled_query.get();
    codegen::QueryCache::Instance().Add(plan, std::move(unique_compiled_query));
  }

  bool query_completed = false;

  auto on_query_result = [&output, &consumer,
                          &query_completed](executor::ExecutionResult result) {
    std::vector<ResultValue> values;
    for (const auto &tuple : consumer.GetOutputTuples()) {
      for (uint32_t i = 0; i < tuple.tuple_.size(); i++) {
        auto column_val = tuple.GetValue(i);
        *output = column_val.GetAs<double>();
        auto str = column_val.IsNull() ? "" : column_val.ToString();
        LOG_TRACE("column content: [%s]", str.c_str());
        values.push_back(std::move(str));
      }
    }
    (void)result;
    query_completed = true;
  };

  compiled_query->Execute(std::move(executor_context), consumer,
                          on_query_result);

  while (!query_completed)
    ;
}
}  // namespace udf
}  // namespace peloton
