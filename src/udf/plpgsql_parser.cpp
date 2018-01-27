#include <sstream>

#include "udf/plpgsql_parser.h"

#include "common/exception.h"
#include "expression/expression_util.h"

namespace peloton {

namespace udf {

const std::string kFunctionList = "FunctionList";
const std::string kAction = "action";
const std::string kDatums = "datums";
const std::string kPLpgSQL_function = "PLpgSQL_function";
const std::string kBody = "body";
const std::string kPLpgSQL_stmt_block = "PLpgSQL_stmt_block";
const std::string kPLpgSQL_stmt_return = "PLpgSQL_stmt_return";
const std::string kPLpgSQL_stmt_if = "PLpgSQL_stmt_if";
const std::string kCond = "cond";
const std::string kThenBody = "then_body";
const std::string kElseBody = "else_body";
const std::string kExpr = "expr";
const std::string kQuery = "query";
const std::string kPLpgSQL_expr = "PLpgSQL_expr";

std::unique_ptr<FunctionAST> PLpgSQLParser::ParsePLpgSQL(
    std::string func_body) {
  auto result = pg_query_parse_plpgsql(func_body.c_str());
  if (result.error) {
    LOG_DEBUG("PL/pgSQL parse error : %s", result.error->message);
    pg_query_free_plpgsql_parse_result(result);
    throw Exception("PL/pgSQL parsing error : " +
                    std::string(result.error->message));
  }
  // The result is a list, we need to wrap it
  std::string ast_json_str = "{ \"" + kFunctionList + "\" : " +
                             std::string(result.plpgsql_funcs) + " }";
  LOG_DEBUG("Compiling JSON formatted function %s", ast_json_str.c_str());
  pg_query_free_plpgsql_parse_result(result);

  std::istringstream ss(ast_json_str);
  Json::Value ast_json;
  ss >> ast_json;
  const auto function_list = ast_json[kFunctionList];
  PL_ASSERT(function_list.isArray());
  if (function_list.size() != 1) {
    LOG_DEBUG("PL/pgSQL error : Function list size %u", function_list.size());
    throw Exception("Function list has size other than 1");
  }
  const auto function_body =
      function_list[0][kPLpgSQL_function][kAction][kPLpgSQL_stmt_block][kBody];
  std::unique_ptr<FunctionAST> function_ast(
      new FunctionAST(ParseBlock(function_body)));
  return function_ast;
}

std::unique_ptr<ExprAST> PLpgSQLParser::ParseBlock(const Json::Value block) {
  // TODO(boweic): Support statements size other than 1
  LOG_DEBUG("Parsing Function Block");
  PL_ASSERT(block.isArray());
  if (block.size() == 0) {
    throw Exception(
        "PL/pgSQL parser : Empty block is not supported");
  }
  const auto stmt = block[0];
  const auto stmt_names = stmt.getMemberNames();
  PL_ASSERT(stmt_names.size() == 1);
  LOG_DEBUG("Statemnt : %s", stmt_names[0].c_str());
  if (stmt_names[0] == kPLpgSQL_stmt_return) {
    // TODO(boweic): Build an ast for return node
    return ParseExprSQL(
        stmt[kPLpgSQL_stmt_return][kExpr][kPLpgSQL_expr][kQuery].asString());
  } else if (stmt_names[0] == kPLpgSQL_stmt_if) {
    return ParseIf(stmt[kPLpgSQL_stmt_if]);
  } else {
    throw Exception("Statement type not supported : " + stmt_names[0]);
  }
}

std::unique_ptr<ExprAST> PLpgSQLParser::ParseIf(const Json::Value branch) {
  LOG_DEBUG("ParseIf");
  auto cond_expr =
      ParseExprSQL(branch[kCond][kPLpgSQL_expr][kQuery].asString());
  auto then_stmt = ParseBlock(branch[kThenBody]);
  auto else_stmt = ParseBlock(branch[kElseBody]);
  return std::unique_ptr<IfExprAST>(new IfExprAST(
      std::move(cond_expr), std::move(then_stmt), std::move(else_stmt)));
}

std::unique_ptr<ExprAST> PLpgSQLParser::ParseExprSQL(
    const std::string expr_sql_str) {
  LOG_DEBUG("Parsing Expr SQL : %s", expr_sql_str.c_str());
  auto &parser = parser::PostgresParser::GetInstance();
  auto stmt_list = parser.BuildParseTree(expr_sql_str.c_str());
  PL_ASSERT(stmt_list->GetNumStatements() == 1);
  auto stmt = stmt_list->GetStatement(0);
  PL_ASSERT(stmt->GetType() == StatementType::SELECT);
  PL_ASSERT(reinterpret_cast<parser::SelectStatement *>(stmt)->from_table ==
            nullptr);
  auto &select_list =
      reinterpret_cast<parser::SelectStatement *>(stmt)->select_list;
  PL_ASSERT(select_list.size() == 1);
  return PLpgSQLParser::ParseExpr(select_list[0].get());
}

std::unique_ptr<ExprAST> PLpgSQLParser::ParseExpr(
    const expression::AbstractExpression *expr) {
  if (expr->GetExpressionType() == ExpressionType::VALUE_TUPLE) {
    return std::unique_ptr<VariableExprAST>(new VariableExprAST(
        reinterpret_cast<const expression::TupleValueExpression *>(expr)
            ->GetColumnName()));
  } else if (expression::ExpressionUtil::IsOperatorExpression(
                 expr->GetExpressionType()) ||
             expr->GetExpressionType() == ExpressionType::COMPARE_LESSTHAN ||
             expr->GetExpressionType() == ExpressionType::COMPARE_GREATERTHAN) {
    switch (expr->GetExpressionType()) {
      case ExpressionType::OPERATOR_PLUS:
        return std::unique_ptr<BinaryExprAST>(new BinaryExprAST(
            '+', ParseExpr(expr->GetChild(0)), ParseExpr(expr->GetChild(1))));
      case ExpressionType::OPERATOR_MINUS:
        return std::unique_ptr<BinaryExprAST>(new BinaryExprAST(
            '-', ParseExpr(expr->GetChild(0)), ParseExpr(expr->GetChild(1))));
      case ExpressionType::OPERATOR_MULTIPLY:
        return std::unique_ptr<BinaryExprAST>(new BinaryExprAST(
            '*', ParseExpr(expr->GetChild(0)), ParseExpr(expr->GetChild(1))));
      case ExpressionType::OPERATOR_DIVIDE:
        return std::unique_ptr<BinaryExprAST>(new BinaryExprAST(
            '/', ParseExpr(expr->GetChild(0)), ParseExpr(expr->GetChild(1))));
      case ExpressionType::COMPARE_LESSTHAN:
        return std::unique_ptr<BinaryExprAST>(new BinaryExprAST(
            '<', ParseExpr(expr->GetChild(0)), ParseExpr(expr->GetChild(1))));
      case ExpressionType::COMPARE_GREATERTHAN:
        return std::unique_ptr<BinaryExprAST>(new BinaryExprAST(
            '>', ParseExpr(expr->GetChild(0)), ParseExpr(expr->GetChild(1))));
      default:
        PL_ASSERT(false);
    }
  } else if (expr->GetExpressionType() == ExpressionType::FUNCTION) {
    auto func_expr =
        reinterpret_cast<const expression::FunctionExpression *>(expr);
    std::vector<std::unique_ptr<ExprAST>> args;
    auto num_args = func_expr->GetChildrenSize();
    for (size_t idx = 0; idx < num_args; ++idx) {
      args.push_back(ParseExpr(func_expr->GetChild(idx)));
    }
    return std::unique_ptr<CallExprAST>(
        new CallExprAST(func_expr->GetFuncName(), std::move(args),
                        curr_func_name_, curr_args_type_));
  } else if (expr->GetExpressionType() == ExpressionType::VALUE_CONSTANT) {
    auto value =
        reinterpret_cast<const expression::ConstantValueExpression *>(expr)
            ->GetValue();
    // TODO(boweic): support other types
    if (!value.CheckInteger()) {
      throw Exception("PLpgSQLParser : Type " + value.GetInfo() +
                      " Not supported");
    }
    return std::unique_ptr<NumberExprAST>(
        new NumberExprAST(value.GetAs<int>()));
  }
  throw Exception("PL/pgSQL parser : Expression type not supported");
}
}  // namespace udf
}  // namespace peloton
