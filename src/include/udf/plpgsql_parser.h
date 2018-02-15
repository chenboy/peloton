#include <memory>
#include <vector>

#pragma once

#include <jsoncpp/jsoncpp.h>
#include "udf/ast_nodes.h"
#include "udf/udf_context.h"

namespace peloton {

namespace udf {

class FunctionAST;
class PLpgSQLParser {
 public:
  PLpgSQLParser(UDFContext *udf_context)
      : udf_context_(udf_context) {};
  std::unique_ptr<FunctionAST> ParsePLpgSQL(std::string func_body);

 private:
  std::unique_ptr<StmtAST> ParseBlock(const Json::Value block);
  std::unique_ptr<StmtAST> ParseFunction(const Json::Value block);
  std::unique_ptr<StmtAST> ParseDecl(const Json::Value decl);
  std::unique_ptr<StmtAST> ParseIf(const Json::Value branch);
  std::unique_ptr<StmtAST> ParseWhile(const Json::Value loop);
  // Feed the expression (as a sql string) to our parser then transform the
  // peloton expression into ast node
  std::unique_ptr<ExprAST> ParseExprSQL(const std::string expr_sql_str);
  std::unique_ptr<ExprAST> ParseExpr(const expression::AbstractExpression *);

  UDFContext *udf_context_;

};
}  // namespace udf
}  // namespace peloton
