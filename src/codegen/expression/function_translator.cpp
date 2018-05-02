//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// function_translator.cpp
//
// Identification: src/codegen/expression/function_translator.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/expression/function_translator.h"

#include "codegen/proxy/string_functions_proxy.h"
#include "codegen/type/decimal_type.h"
#include "codegen/type/integer_type.h"
#include "codegen/type/sql_type.h"
#include "codegen/type/type_system.h"
#include "codegen/type/varchar_type.h"
#include "expression/function_expression.h"
#include "type/type_id.h"
#include "udf/udf_handler.h"

namespace peloton {
namespace codegen {

FunctionTranslator::FunctionTranslator(
    const expression::FunctionExpression &func_expr,
    CompilationContext &context)
    : ExpressionTranslator(func_expr, context) {
  if (!func_expr.IsUDF()) {
    PL_ASSERT(func_expr.GetFunc().op_id != OperatorId::Invalid);
    PL_ASSERT(func_expr.GetFunc().impl != nullptr);

    // Prepare each of the child expressions
    for (uint32_t i = 0; i < func_expr.GetChildrenSize(); i++) {
      context.Prepare(*func_expr.GetChild(i));
    }
  }
}

codegen::Value FunctionTranslator::DeriveValue(CodeGen &codegen,
                                               RowBatch::Row &row) const {
  // The function expression
  const auto &func_expr = GetExpressionAs<expression::FunctionExpression>();

  // Collect the arguments for the function
  std::vector<codegen::Value> args;
  for (uint32_t i = 0; i < func_expr.GetChildrenSize(); i++) {
    args.push_back(row.DeriveValue(codegen, *func_expr.GetChild(i)));
  }

  // The context for the function invocation
  type::TypeSystem::InvocationContext ctx{
      .on_error = OnError::Exception,
      .executor_context = context_.GetExecutorContextPtr()};

  if (!func_expr.IsUDF()) {
    // The ID of the operator we're calling
    OperatorId operator_id = func_expr.GetFunc().op_id;

    if (args.size() == 1) {
      // Lookup unary operation
      auto *unary_op =
          type::TypeSystem::GetUnaryOperator(operator_id, args[0].GetType());
      PL_ASSERT(unary_op != nullptr);

      // Invoke
      return unary_op->Eval(codegen, args[0], ctx);
    } else if (args.size() == 2) {
      // Lookup the function
      type::Type left_type = args[0].GetType(), right_type = args[1].GetType();
      auto *binary_op = type::TypeSystem::GetBinaryOperator(
          operator_id, left_type, left_type, right_type, right_type);
      PL_ASSERT(binary_op);

      // Invoke
      return binary_op->Eval(codegen, args[0].CastTo(codegen, left_type),
                             args[1].CastTo(codegen, right_type), ctx);
    } else {
      // It's an N-Ary function
      // Collect argument types for lookup
      std::vector<type::Type> arg_types;
      for (const auto &arg_val : args) {
        arg_types.push_back(arg_val.GetType());
      }

      // Lookup the function
      auto *nary_op = type::TypeSystem::GetNaryOperator(operator_id, arg_types);
      PL_ASSERT(nary_op != nullptr);

      // Invoke
      return nary_op->Eval(codegen, args, ctx);
    }
  } else {
    // It's a UDF
    std::vector<llvm::Value *> raw_args;
    for (uint32_t i = 0; i < args.size(); i++) {
      if(args[i].GetType().type_id == peloton::type::TypeId::VARCHAR) {
        // create a StrWithLenProxy struct
        // TODO[Siva]: Is this the right way to do it?
        LOG_INFO("HERE 0");
        auto code_context = std::make_shared<codegen::CodeContext>();
        LOG_INFO("HERE 1");
        codegen::CodeGen cg{*code_context};
        LOG_INFO("HERE 2");
        auto *str_with_len_type = peloton::codegen::StrWithLenProxy::GetType(
                                      cg);
        LOG_INFO("HERE 3");
        llvm::Value *agg_val = cg.AllocateVariable(str_with_len_type,
                                      "argi");

        LOG_INFO("HERE 4");
        std::vector<llvm::Value*> indices(2);
        indices[0] = cg.Const32(0);
        indices[1] = cg.Const32(0);

        LOG_INFO("HERE 5");
        auto *str_ptr = cg->CreateGEP(str_with_len_type, agg_val, indices,
                                      "arg_str_ptr");

        indices[1] = cg.Const32(1);
        LOG_INFO("HERE 6");
        auto *str_len = cg->CreateGEP(str_with_len_type, agg_val, indices,
                                      "arg_str_len");

        LOG_INFO("HERE 7");
        cg->CreateStore(args[i].GetValue(), str_ptr);
        LOG_INFO("HERE 8");
        cg->CreateStore(args[i].GetLength(), str_len);
        LOG_INFO("HERE 9");
        agg_val = cg->CreateLoad(agg_val);
        LOG_INFO("HERE 10");
        raw_args.push_back(agg_val);
        LOG_INFO("HERE 11");
      } else {
        raw_args.push_back(args[i].GetValue());
      }
    }

    std::unique_ptr<peloton::udf::UDFHandler> udf_handler(
        new peloton::udf::UDFHandler());

    // Register function prototype in current context
    auto *func_ptr = udf_handler->RegisterExternalFunction(codegen, func_expr);

    auto call_ret = codegen.CallFunc(func_ptr, raw_args);

    auto return_type = func_expr.GetValueType();

    // TODO[Siva]: Support more datatypes for return value
    switch (return_type) {
      case peloton::type::TypeId::DECIMAL : {
        return codegen::Value{type::Decimal::Instance(), call_ret, nullptr,
                          nullptr};
      }
      case peloton::type::TypeId::INTEGER : {
        return codegen::Value{type::Integer::Instance(), call_ret, nullptr,
                          nullptr};
      }
      case peloton::type::TypeId::VARCHAR : {
        llvm::Value *str_ptr = codegen->CreateExtractValue(call_ret, 0);
        llvm::Value *str_len = codegen->CreateExtractValue(call_ret, 1);
        return codegen::Value{type::Varchar::Instance(), str_ptr, str_len,
                          nullptr};
      }
      default : {
        throw Exception("FunctionTranslator : Return type not supported");
      }
    }
  }
}

}  // namespace codegen
}  // namespace peloton
