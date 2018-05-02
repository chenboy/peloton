//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// udf_codegen.cpp
//
// Identification: src/udf/udf_codegen.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "udf/udf_codegen.h"

#include "catalog/catalog.h"
#include "codegen/buffering_consumer.h"
#include "codegen/lang/if.h"
#include "codegen/lang/loop.h"
#include "codegen/proxy/udf_util_proxy.h"
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
#include "udf/ast_nodes.h"
#include "udf/udf_util.h"

namespace peloton {
namespace udf {
UDFCodegen::UDFCodegen(codegen::CodeGen *codegen, codegen::FunctionBuilder *fb,
                       UDFContext *udf_context)
    : codegen_(codegen), fb_(fb), udf_context_(udf_context), dst_(nullptr){};

void UDFCodegen::GenerateUDF(AbstractAST *ast) { ast->Accept(this); }

void UDFCodegen::Visit(ValueExprAST *ast) {
  switch (ast->value_.GetTypeId()) {
    case type::TypeId::INTEGER: {
      *dst_ = codegen::Value(codegen::type::Type(type::TypeId::INTEGER, false),
                             codegen_->Const32(ast->value_.GetAs<int>()));
      break;
    }
    case type::TypeId::DECIMAL: {
      *dst_ = peloton::codegen::Value(
          peloton::codegen::type::Type(type::TypeId::DECIMAL, false),
          codegen_->ConstDouble(ast->value_.GetAs<double>()));
      break;
    }
    case type::TypeId::VARCHAR: {
      auto *val = codegen_->ConstStringPtr(ast->value_.ToString());
      auto *len = codegen_->Const32(ast->value_.GetLength());
      *dst_ = peloton::codegen::Value(
          peloton::codegen::type::Type(type::TypeId::VARCHAR, false), val, len);
      break;
    }
    default:
      throw Exception("ValueExprAST::Codegen : Expression type not supported");
  }
}

void UDFCodegen::Visit(VariableExprAST *ast) {
  llvm::Value *val = fb_->GetArgumentByName(ast->name);
  type::TypeId type = udf_context_->GetVariableType(ast->name);

  if (val != nullptr) {
    if (type == type::TypeId::VARCHAR) {
      // read in from the StrwithLen
      auto *str_with_len_type = peloton::codegen::StrWithLenProxy::GetType(
                                    *codegen_);

      std::vector<llvm::Value*> indices(2);
      indices[0] = codegen_->Const32(0);
      indices[1] = codegen_->Const32(0);

      auto *str_val = (*codegen_)->CreateLoad((*codegen_)->CreateGEP(
          str_with_len_type, val, indices, "str_ptr"));

      indices[1] = codegen_->Const32(1);
      auto *len_val = (*codegen_)->CreateLoad((*codegen_)->CreateGEP(
          str_with_len_type, val,indices, "str_len"));

      *dst_ = peloton::codegen::Value(
          peloton::codegen::type::Type(type::TypeId::VARCHAR, false), str_val,
          len_val);
    } else {
      *dst_ = codegen::Value(codegen::type::Type(type, false), val);
      return;
    }
  } else {
    if (type == type::TypeId::VARCHAR) {
      auto alloc_val = udf_context_->GetAllocValue(ast->name);
      llvm::Value *val = nullptr, *len = nullptr, *null = nullptr;
      alloc_val.ValuesForMaterialization(*codegen_, val, len, null);

      auto *index = codegen_->Const32(0);
      auto *str_val = (*codegen_)->CreateLoad(
          (*codegen_)->CreateInBoundsGEP(codegen_->CharPtrType(), val, index));
      auto *len_val = (*codegen_)->CreateLoad(
          (*codegen_)->CreateInBoundsGEP(codegen_->Int32Type(), len, index));

      *dst_ = peloton::codegen::Value(
          peloton::codegen::type::Type(type::TypeId::VARCHAR, false), str_val,
          len_val);
    } else {
      // Assuming each variable is defined
      auto *alloc_val = (udf_context_->GetAllocValue(ast->name)).GetValue();
      *dst_ = codegen::Value(codegen::type::Type(type, false),
                             (*codegen_)->CreateLoad(alloc_val));
      return;
    }
  }
}

void UDFCodegen::Visit(BinaryExprAST *ast) {
  auto *ret_dst = dst_;
  codegen::Value left;
  dst_ = &left;
  ast->lhs->Accept(this);
  codegen::Value right;
  dst_ = &right;
  ast->rhs->Accept(this);
  // TODO(boweic): Should not be nullptr;
  if (left.GetValue() == nullptr || right.GetValue() == nullptr) {
    *ret_dst = codegen::Value();
    return;
  }
  switch (ast->op) {
    case ExpressionType::OPERATOR_PLUS: {
      *ret_dst = left.Add(*codegen_, right);
      return;
    }
    case ExpressionType::OPERATOR_MINUS: {
      *ret_dst = left.Sub(*codegen_, right);
      return;
    }
    case ExpressionType::OPERATOR_MULTIPLY: {
      *ret_dst = left.Mul(*codegen_, right);
      return;
    }
    case ExpressionType::OPERATOR_DIVIDE: {
      *ret_dst = left.Div(*codegen_, right);
      return;
    }
    case ExpressionType::COMPARE_LESSTHAN: {
      auto val = left.CompareLt(*codegen_, right);
      // TODO(boweic): support boolean type
      *ret_dst = val.CastTo(*codegen_,
                            codegen::type::Type(type::TypeId::DECIMAL, false));
      return;
    }
    case ExpressionType::COMPARE_GREATERTHAN: {
      auto val = left.CompareGt(*codegen_, right);
      *ret_dst = val.CastTo(*codegen_,
                            codegen::type::Type(type::TypeId::DECIMAL, false));
      return;
    }
    case ExpressionType::COMPARE_EQUAL: {
      auto val = left.CompareEq(*codegen_, right);
      *ret_dst = val.CastTo(*codegen_,
                            codegen::type::Type(type::TypeId::DECIMAL, false));
      return;
    }
    default:
      throw Exception("BinaryExprAST : Operator not supported");
  }
}

void UDFCodegen::Visit(CallExprAST *ast) {
  std::vector<llvm::Value *> args_val;
  std::vector<type::TypeId> args_type;
  auto *ret_dst = dst_;
  // Codegen type needed to retrieve built-in functions
  // TODO(boweic): Use a uniform API for UDF and built-in so code don't get
  // super ugly
  std::vector<codegen::Value> args_codegen_val;
  for (unsigned i = 0, size = ast->args.size(); i != size; ++i) {
    codegen::Value arg_val;
    dst_ = &arg_val;
    ast->args[i]->Accept(this);
    args_val.push_back(arg_val.GetValue());
    // TODO(boweic): Handle type missmatch in typechecking phase
    args_type.push_back(arg_val.GetType().type_id);
    args_codegen_val.push_back(arg_val);
  }

  // Check if present in the current code context
  // Else, check the catalog and get it
  llvm::Function *callee_func;
  type::TypeId return_type = type::TypeId::INVALID;
  if (ast->callee == udf_context_->GetFunctionName()) {
    // Recursive function call
    callee_func = fb_->GetFunction();
    return_type = udf_context_->GetFunctionReturnType();
  } else {
    // Check and set the function ptr
    // TODO(boweic): Visit the catalog using the interface that is protected by
    // transaction
    const catalog::FunctionData &func_data =
        catalog::Catalog::GetInstance()->GetFunction(ast->callee, args_type);
    if (func_data.is_udf_) {
      return_type = func_data.return_type_;
      llvm::Type *ret_type =
          UDFUtil::GetCodegenType(func_data.return_type_, *codegen_);
      std::vector<llvm::Type *> llvm_args;
      for (const auto &arg_type : args_type) {
        llvm_args.push_back(UDFUtil::GetCodegenType(arg_type, *codegen_));
      }
      auto *fn_type = llvm::FunctionType::get(ret_type, llvm_args, false);
      callee_func = llvm::Function::Create(
          fn_type, llvm::Function::ExternalLinkage, ast->callee,
          &(codegen_->GetCodeContext().GetModule()));
      codegen_->GetCodeContext().RegisterExternalFunction(
          callee_func, func_data.func_context_->GetRawFunctionPointer(
                           func_data.func_context_->GetUDF()));
    } else {
      codegen::type::TypeSystem::InvocationContext ctx{
          .on_error = OnError::Exception, .executor_context = nullptr};
      OperatorId operator_id = func_data.func_.op_id;
      if (ast->args.size() == 1) {
        auto *unary_op = codegen::type::TypeSystem::GetUnaryOperator(
            operator_id, args_codegen_val[0].GetType());
        *ret_dst = unary_op->Eval(*codegen_, args_codegen_val[0], ctx);
        PL_ASSERT(unary_op != nullptr);
        return;
      } else if (ast->args.size() == 2) {
        codegen::type::Type left_type = args_codegen_val[0].GetType(),
                            right_type = args_codegen_val[1].GetType();
        auto *binary_op = codegen::type::TypeSystem::GetBinaryOperator(
            operator_id, left_type, right_type, left_type, right_type);
        *ret_dst = binary_op->Eval(
            *codegen_, args_codegen_val[0].CastTo(*codegen_, left_type),
            args_codegen_val[1].CastTo(*codegen_, right_type), ctx);
        PL_ASSERT(binary_op != nullptr);
        return;
      } else {
        std::vector<codegen::type::Type> args_codegen_type;
        for (const auto &val : args_codegen_val) {
          args_codegen_type.push_back(val.GetType());
        }
        auto *nary_op = codegen::type::TypeSystem::GetNaryOperator(
            operator_id, args_codegen_type);
        PL_ASSERT(nary_op != nullptr);
        *ret_dst = nary_op->Eval(*codegen_, args_codegen_val, ctx);
        return;
      }
    }
  }

  // TODO(boweic): Throw an exception?
  if (callee_func == nullptr) {
    return;  // LogErrorV("Unknown function referenced");
  }

  // TODO(boweic): Do this in typechecking
  if (callee_func->arg_size() != ast->args.size()) {
    return;  // LogErrorV("Incorrect # arguments passed");
  }

  auto *call_ret = codegen_->CallFunc(callee_func, args_val);

  // TODO(boweic): Maybe wrap this logic as a helper function since it could be
  // reused
  switch (return_type) {
    case type::TypeId::DECIMAL: {
      *ret_dst = codegen::Value{codegen::type::Decimal::Instance(), call_ret};
      break;
    }
    case type::TypeId::INTEGER: {
      *ret_dst = codegen::Value{codegen::type::Integer::Instance(), call_ret};
      break;
    }
    default: {
      throw Exception("CallExpr::Codegen : Return type not supported");
    }
  }
}

void UDFCodegen::Visit(SeqStmtAST *ast) {
  for (uint32_t i = 0; i < ast->stmts.size(); i++) {
    // If already return in the current block, don't continue to generate
    if (codegen_->IsTerminated()) {
      break;
    }
    ast->stmts[i]->Accept(this);
  }
}

void UDFCodegen::Visit(DeclStmtAST *ast) {
  switch (ast->type) {
    // TODO[Siva]: This is a hack, this is a pointer to type, not type
    // TODO[Siva]: Replace with this a function that returns llvm::Type from
    // type::TypeID
    case type::TypeId::INTEGER: {
      // TODO[Siva]: 32 / 64 bit handling??
      auto alloc_val = peloton::codegen::Value(
          peloton::codegen::type::Type(type::TypeId::INTEGER, false),
          codegen_->AllocateVariable(codegen_->Int32Type(), ast->name));
      udf_context_->SetAllocValue(ast->name, alloc_val);
      break;
    }
    case type::TypeId::DECIMAL: {
      auto alloc_val = peloton::codegen::Value(
          peloton::codegen::type::Type(type::TypeId::DECIMAL, false),
          codegen_->AllocateVariable(codegen_->DoubleType(), ast->name));
      udf_context_->SetAllocValue(ast->name, alloc_val);
      break;
    }
    case type::TypeId::VARCHAR: {
      auto alloc_val = peloton::codegen::Value(
          peloton::codegen::type::Type(type::TypeId::VARCHAR, false),
          codegen_->AllocateVariable(codegen_->CharPtrType(),
                                     ast->name + "_ptr"),
          codegen_->AllocateVariable(codegen_->Int32Type(),
                                     ast->name + "_len"));
      udf_context_->SetAllocValue(ast->name, alloc_val);
      break;
    }
    default: {
      // TODO[Siva]: Should throw an excpetion, but need to figure out "found"
      // and other internal types first.
    }
  }
}

void UDFCodegen::Visit(IfStmtAST *ast) {
  auto compare_value = peloton::codegen::Value(
      peloton::codegen::type::Type(type::TypeId::DECIMAL, false),
      codegen_->ConstDouble(1.0));

  peloton::codegen::Value cond_expr_value;
  dst_ = &cond_expr_value;
  ast->cond_expr->Accept(this);

  // Codegen If condition expression
  codegen::lang::If entry_cond{
      *codegen_, cond_expr_value.CompareEq(*codegen_, compare_value),
      "entry_cond"};
  {
    // Codegen the then statements
    ast->then_stmt->Accept(this);
  }
  entry_cond.ElseBlock("multipleValue");
  {
    // codegen the else statements
    ast->else_stmt->Accept(this);
  }
  entry_cond.EndIf();

  return;
}

void UDFCodegen::Visit(WhileStmtAST *ast) {
  // TODO(boweic): Use boolean when supported
  auto compare_value =
      codegen::Value(codegen::type::Type(type::TypeId::DECIMAL, false),
                     codegen_->ConstDouble(1.0));

  peloton::codegen::Value cond_expr_value;
  dst_ = &cond_expr_value;
  ast->cond_expr->Accept(this);

  // Codegen If condition expression
  codegen::lang::Loop loop{
      *codegen_,
      cond_expr_value.CompareEq(*codegen_, compare_value).GetValue(),
      {}};
  {
    ast->body_stmt->Accept(this);
    // TODO(boweic): Use boolean when supported
    auto compare_value = peloton::codegen::Value(
        codegen::type::Type(type::TypeId::DECIMAL, false),
        codegen_->ConstDouble(1.0));
    codegen::Value cond_expr_value;
    codegen::Value cond_var;
    if (!codegen_->IsTerminated()) {
      dst_ = &cond_expr_value;
      ast->cond_expr->Accept(this);
      cond_var = cond_expr_value.CompareEq(*codegen_, compare_value);
    }
    loop.LoopEnd(cond_var.GetValue(), {});
  }

  return;
}

void UDFCodegen::Visit(RetStmtAST *ast) {
  // TODO[Siva]: Handle void properly
  if (ast->expr == nullptr) {
    // TODO(boweic): We should deduce type in typechecking phase and create a
    // default value for that type, or find a way to get around llvm basic
    // block
    // without return
    codegen::Value value = peloton::codegen::Value(
        peloton::codegen::type::Type(udf_context_->GetFunctionReturnType(),
                                     false),
        codegen_->ConstDouble(0));
    (*codegen_)->CreateRet(value.GetValue());
  } else {
    codegen::Value expr_ret_val;
    dst_ = &expr_ret_val;
    ast->expr->Accept(this);

    if (expr_ret_val.GetType() !=
        peloton::codegen::type::Type(udf_context_->GetFunctionReturnType(),
                                     false)) {
      expr_ret_val = expr_ret_val.CastTo(
          *codegen_,
          peloton::codegen::type::Type(udf_context_->GetFunctionReturnType(), false));
    }

    if(expr_ret_val.GetType() ==
          peloton::codegen::type::Type(type::TypeId::VARCHAR, false)) {
      auto *str_with_len_type = peloton::codegen::StrWithLenProxy::GetType(
                                    *codegen_);
      llvm::Value *agg_val = codegen_->AllocateVariable(str_with_len_type,
                                    "return_val");

      std::vector<llvm::Value*> indices(2);
      indices[0] = codegen_->Const32(0);
      indices[1] = codegen_->Const32(0);

      auto *str_ptr = (*codegen_)->CreateGEP(str_with_len_type, agg_val,
                                             indices, "str_ptr");

      indices[1] = codegen_->Const32(1);
      auto *str_len = (*codegen_)->CreateGEP(str_with_len_type, agg_val,
                                             indices, "str_len");

      (*codegen_)->CreateStore(expr_ret_val.GetValue(), str_ptr);
      (*codegen_)->CreateStore(expr_ret_val.GetLength(), str_len);
      agg_val = (*codegen_)->CreateLoad(agg_val);
      (*codegen_)->CreateRet(agg_val);
    } else {
      (*codegen_)->CreateRet(expr_ret_val.GetValue());
    }
  }
}

void UDFCodegen::Visit(AssignStmtAST *ast) {
  codegen::Value right_codegen_val;
  dst_ = &right_codegen_val;
  ast->rhs->Accept(this);

  auto left_codegen_val = ast->lhs->GetAllocValue(udf_context_);

  auto *left_val = left_codegen_val.GetValue();

  auto right_type = right_codegen_val.GetType();

  auto left_type_id = ast->lhs->GetVarType(udf_context_);

  auto left_type = codegen::type::Type(left_type_id, false);

  if (left_type_id == type::TypeId::VARCHAR) {
    llvm::Value *l_val = nullptr, *l_len = nullptr, *l_null = nullptr;
    left_codegen_val.ValuesForMaterialization(*codegen_, l_val, l_len, l_null);

    llvm::Value *r_val = nullptr, *r_len = nullptr, *r_null = nullptr;
    right_codegen_val.ValuesForMaterialization(*codegen_, r_val, r_len, r_null);

    (*codegen_)->CreateStore(r_val, l_val);
    (*codegen_)->CreateStore(r_len, l_len);

    return;
  }

  if (right_type != left_type) {
    // TODO[Siva]: Need to check that they can be casted in semantic analysis
    right_codegen_val = right_codegen_val.CastTo(
        *codegen_,
        codegen::type::Type(ast->lhs->GetVarType(udf_context_), false));
  }

  (*codegen_)->CreateStore(right_codegen_val.GetValue(), left_val);
}

void UDFCodegen::Visit(SQLStmtAST *ast) {
  auto *val = codegen_->ConstStringPtr(ast->query);
  auto *len = codegen_->Const32(ast->query.size());
  auto left = udf_context_->GetAllocValue(ast->var_name);

  // auto &code_context = codegen_->GetCodeContext();

  // codegen::FunctionBuilder temp_fun{
  //     code_context, "temp_fun_1", codegen_->DoubleType(), {}};
  // {
  //   temp_fun.ReturnAndFinish(codegen_->ConstDouble(12.0));
  // }

  // auto *right_val = codegen_->CallFunc(temp_fun.GetFunction(), {});
  // (*codegen_)->CreateStore(right_val, left.GetValue());

  codegen_->Call(codegen::UDFUtilProxy::ExecuteSQLHelper,
                 {val, len, left.GetValue()});
  return;
}

void UDFCodegen::Visit(DynamicSQLStmtAST *ast) {
  codegen::Value query;
  dst_ = &query;
  ast->query->Accept(this);
  auto left = udf_context_->GetAllocValue(ast->var_name);
  codegen_->Call(codegen::UDFUtilProxy::ExecuteSQLHelper,
                 {query.GetValue(), query.GetLength(), left.GetValue()});
  return;
}

}  // namespace udf
}  // namespace peloton
