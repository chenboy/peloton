#include <memory>
#include <vector>

#pragma once

#include <jsoncpp/jsoncpp.h>
#include "udf/ast_nodes.h"

namespace peloton {

namespace udf {

class FunctionAST;
class PLpgSQLParser {
 public:
  PLpgSQLParser(std::string func_name, std::vector<type::TypeId> args_type)
      : curr_func_name_(func_name), curr_args_type_(args_type){};
  std::unique_ptr<FunctionAST> ParsePLpgSQL(std::string func_body);

 private:
  std::unique_ptr<StmtAST> ParseBlock(const Json::Value block);
  std::unique_ptr<StmtAST> ParseFunction(const Json::Value block);
  std::unique_ptr<StmtAST> ParseDecl(const Json::Value decl);
  std::unique_ptr<StmtAST> ParseIf(const Json::Value branch);
  // Feed the expression (as a sql string) to our parser then transform the
  // peloton expression into ast node
  std::unique_ptr<ExprAST> ParseExprSQL(const std::string expr_sql_str);
  std::unique_ptr<ExprAST> ParseExpr(const expression::AbstractExpression *);

  std::string curr_func_name_;
  std::vector<type::TypeId> curr_args_type_;
};
}  // namespace udf
}  // namespace peloton
