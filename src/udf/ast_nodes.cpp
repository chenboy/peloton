#include "udf/ast_nodes.h"
#include <iostream>  ////TODO(PP) Remove
#include "catalog/catalog.h"
#include "codegen/lang/if.h"
#include "codegen/lang/loop.h"
#include "codegen/type/type.h"

namespace peloton {
namespace udf {

// Codegen for SeqStmtAST
void SeqStmtAST::Codegen(codegen::CodeGen &codegen,
                         codegen::FunctionBuilder &fb,
                         UNUSED_ATTRIBUTE codegen::Value *dst,
                         UDFContext *udf_context) {
  for (uint32_t i = 0; i < stmts.size(); i++) {
    // If already return in the current block, don't continue to generate
    if (codegen.IsTerminated()) {
      break;
    }
    stmts[i]->Codegen(codegen, fb, nullptr, udf_context);
  }

  return;
}

// Codegen for DeclStmtAST
void DeclStmtAST::Codegen(
    peloton::codegen::CodeGen &codegen,
    UNUSED_ATTRIBUTE peloton::codegen::FunctionBuilder &fb,
    UNUSED_ATTRIBUTE codegen::Value *dst, UDFContext *udf_context) {
  switch (type) {
    case type::TypeId::INTEGER : {
      // TODO[Siva]: 32 / 64 bit handling?? 
      udf_context->SetAllocValue(name,
          codegen.AllocateVariable(codegen.Int64Type(), name));
      break;
    }
    case type::TypeId::DECIMAL : {
      udf_context->SetAllocValue(name,
          codegen.AllocateVariable(codegen.DoubleType(), name));
      break;
    }
    default : {
      // TODO[Siva]: Should throw an excpetion, but need to figure out "found"
      // and other internal types first.
    }
  }
  return;
}

// Codegen for IntegerExprAST
void IntegerExprAST::Codegen(
    peloton::codegen::CodeGen &codegen,
    UNUSED_ATTRIBUTE peloton::codegen::FunctionBuilder &fb,
    codegen::Value *dst, UNUSED_ATTRIBUTE UDFContext *udf_context) {
  *dst = peloton::codegen::Value(
      peloton::codegen::type::Type(type::TypeId::INTEGER, false),
      codegen.Const64(val));
  return;
}

// Codegen for DoubleExprAST
void DoubleExprAST::Codegen(
    peloton::codegen::CodeGen &codegen,
    UNUSED_ATTRIBUTE peloton::codegen::FunctionBuilder &fb,
    codegen::Value *dst, UNUSED_ATTRIBUTE UDFContext *udf_context) {
  *dst = peloton::codegen::Value(
      peloton::codegen::type::Type(type::TypeId::DECIMAL, false),
      codegen.ConstDouble(val));
  return;
}

// Codegen for VariableExprAST
void VariableExprAST::Codegen(
    UNUSED_ATTRIBUTE peloton::codegen::CodeGen &codegen,
    peloton::codegen::FunctionBuilder &fb, codegen::Value *dst,
    UDFContext *udf_context) {
  llvm::Value *val = fb.GetArgumentByName(name);
  type::TypeId type = udf_context->GetVariableType(name);
  //TODO[Siva]: Support Integers for arguments as well
  if (val) {
    *dst = peloton::codegen::Value(
        peloton::codegen::type::Type(type::TypeId::DECIMAL, false), val);
    return;
  } else {
    // Assuming each variable is defined
    *dst = peloton::codegen::Value(peloton::codegen::type::Type(type, false),
        codegen->CreateLoad(udf_context->GetAllocValue(name)));
    return;
  }
  return;
}

// Codegen for BinaryExprAST
void BinaryExprAST::Codegen(codegen::CodeGen &codegen,
                            codegen::FunctionBuilder &fb, codegen::Value *dst,
                            UDFContext *udf_context) {
  codegen::Value left;
  lhs->Codegen(codegen, fb, &left, udf_context);
  codegen::Value right;
  rhs->Codegen(codegen, fb, &right, udf_context);
  // TODO(boweic): Should not be nullptr;
  if (left.GetValue() == nullptr || right.GetValue() == nullptr) {
    *dst = codegen::Value();
    return;
  }

  switch (op) {
    // TODO(boweic): Do not use string
    case '+': {
      *dst = left.Add(codegen, right);
      return;
    }
    case '-': {
      *dst = left.Sub(codegen, right);
      return;
    }
    case '*': {
      *dst = left.Mul(codegen, right);
      return;
    }
    case '/': {
      *dst = left.Div(codegen, right);
      return;
    }
    case '<': {
      auto val = left.CompareLt(codegen, right);
      // TODO(boweic): support boolean type
      *dst = val.CastTo(
          codegen, codegen::type::Type(peloton::type::TypeId::DECIMAL, false));
      return;
    }
    case '>': {
      auto val = left.CompareGt(codegen, right);
      *dst = val.CastTo(
          codegen, codegen::type::Type(peloton::type::TypeId::DECIMAL, false));
      return;
    }
    case '=': {
      auto val = left.CompareEq(codegen, right);
      *dst = val.CastTo(
          codegen, codegen::type::Type(peloton::type::TypeId::DECIMAL, false));
      return;
    }
    default:
      // TODO(boweic): Throw an exception
      return;
  }
}

// Codegen for CallExprAST
void CallExprAST::Codegen(codegen::CodeGen &codegen,
                          codegen::FunctionBuilder &fb, codegen::Value *dst,
                          UDFContext *udf_context) {
  // Check if present in the current code context
  // Else, check the catalog and get it
  auto *callee_func = fb.GetFunction();

  // TODO(boweic): Throw an exception?
  if (callee_func == nullptr) {
    return;  // LogErrorV("Unknown function referenced");
  }

  // TODO(boweic): Do this in typechecking
  if (callee_func->arg_size() != args.size())
    return;  // LogErrorV("Incorrect # arguments passed");

  std::vector<llvm::Value *> args_val;
  for (unsigned i = 0, size = args.size(); i != size; ++i) {
    codegen::Value arg_val;
    args[i]->Codegen(codegen, fb, &arg_val, udf_context);
    args_val.push_back(arg_val.GetValue());
    // TODO(boweic): Should be handled in the typechecking phase
    if (args_val.back() == nullptr) {
      return;  // LogErrorV("Arguments could not be passed in");
    }
  }

  auto *call_ret = codegen.CallFunc(callee_func, args_val);

  *dst = peloton::codegen::Value(
      peloton::codegen::type::Type(type::TypeId::DECIMAL, false), call_ret);

  return;
}

void IfStmtAST::Codegen(codegen::CodeGen &codegen, codegen::FunctionBuilder &fb,
                        UNUSED_ATTRIBUTE codegen::Value *dst,
                        UDFContext *udf_context) {
  PL_ASSERT(dst == nullptr);
  auto compare_value = peloton::codegen::Value(
      peloton::codegen::type::Type(type::TypeId::DECIMAL, false),
      codegen.ConstDouble(1.0));

  peloton::codegen::Value cond_expr_value;
  cond_expr->Codegen(codegen, fb, &cond_expr_value, udf_context);

  // Codegen If condition expression
  codegen::lang::If entry_cond{
      codegen, cond_expr_value.CompareEq(codegen, compare_value), "entry_cond"};
  {
    // Codegen the then statements
    then_stmt->Codegen(codegen, fb, nullptr, udf_context);
  }
  entry_cond.ElseBlock("multipleValue");
  {
    // codegen the else statements
    else_stmt->Codegen(codegen, fb, nullptr, udf_context);
  }
  entry_cond.EndIf();

  return;
}

void WhileStmtAST::Codegen(codegen::CodeGen &codegen,
                           codegen::FunctionBuilder &fb,
                           UNUSED_ATTRIBUTE codegen::Value *dst,
                           UDFContext *udf_context) {
  PL_ASSERT(dst == nullptr);
  // TODO(boweic): Use boolean when supported
  auto compare_value =
      peloton::codegen::Value(codegen::type::Type(type::TypeId::DECIMAL, false),
                              codegen.ConstDouble(1.0));

  peloton::codegen::Value cond_expr_value;
  cond_expr->Codegen(codegen, fb, &cond_expr_value, udf_context);

  // Codegen If condition expression
  codegen::lang::Loop loop{
      codegen,
      cond_expr_value.CompareEq(codegen, compare_value).GetValue(),
      {}};
  {
    body_stmt->Codegen(codegen, fb, nullptr, udf_context);
    // TODO(boweic): Use boolean when supported
    auto compare_value = peloton::codegen::Value(
        codegen::type::Type(type::TypeId::DECIMAL, false),
        codegen.ConstDouble(1.0));
    codegen::Value cond_expr_value;
    codegen::Value cond_var;
    if (!codegen.IsTerminated()) {
      cond_expr->Codegen(codegen, fb, &cond_expr_value, udf_context);
      cond_var = cond_expr_value.CompareEq(codegen, compare_value);
    }
    loop.LoopEnd(cond_var.GetValue(), {});
  }

  return;
}
// Codegen for RetStmtAST
void RetStmtAST::Codegen(codegen::CodeGen &codegen,
                         UNUSED_ATTRIBUTE codegen::FunctionBuilder &fb,
                         UNUSED_ATTRIBUTE codegen::Value *dst,
                         UDFContext *udf_context) {
  // TODO[Siva]: Will need to add more checks to ensure that this is done
  // Handle when supporting types
  if (expr == nullptr) {
    // TODO(boweic): We should deduce type in typechecking phase and create a
    // default value for that type, or find a way to get around llvm basic block
    // without return
    codegen::Value value = peloton::codegen::Value(
        peloton::codegen::type::Type(type::TypeId::DECIMAL, false),
        codegen.ConstDouble(0));
    codegen->CreateRet(value.GetValue());
  } else {
    codegen::Value expr_ret_val;
    expr->Codegen(codegen, fb, &expr_ret_val, udf_context);

    if(expr_ret_val.GetType() != 
       peloton::codegen::type::Type(type::TypeId::DECIMAL, false)) {
      expr_ret_val = expr_ret_val.CastTo(codegen,
        codegen::type::Type(type::TypeId::DECIMAL, false));
    }

    codegen->CreateRet(expr_ret_val.GetValue());
  }
  return;
}

// Codegen for AssignStmtAST
void AssignStmtAST::Codegen(codegen::CodeGen &codegen,
                            UNUSED_ATTRIBUTE codegen::FunctionBuilder &fb,
                            UNUSED_ATTRIBUTE codegen::Value *dst,
                            UDFContext *udf_context) {
  codegen::Value right_codegen_val;
  rhs->Codegen(codegen, fb, &right_codegen_val, udf_context);
  auto *left_val = lhs->GetAllocValue(udf_context);
  auto right_type = right_codegen_val.GetType();
  auto left_type = codegen::type::Type(lhs->GetVarType(udf_context), false);

  if(right_type != left_type) {
    // TODO[Siva]: Need to check that they can be casted in semantic analysis
    right_codegen_val = right_codegen_val.CastTo(codegen,
        codegen::type::Type(lhs->GetVarType(udf_context), false));
  }

  codegen->CreateStore(right_codegen_val.GetValue(), left_val);
}

// Codegen for FunctionAST
llvm::Function *FunctionAST::Codegen(peloton::codegen::CodeGen &codegen,
                                     peloton::codegen::FunctionBuilder &fb,
                                     UDFContext *udf_context) {
  body->Codegen(codegen, fb, nullptr, udf_context);
  fb.Finish();
  return fb.GetFunction();
}

std::unique_ptr<ExprAST> LogError(UNUSED_ATTRIBUTE const char *str) {
  LOG_TRACE("Error: %s\n", str);
  return 0;
}

peloton::codegen::Value LogErrorV(const char *str) {
  LogError(str);
  return peloton::codegen::Value();
}

}  // namespace udf
}  // namespace peloton
