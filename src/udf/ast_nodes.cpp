#include "udf/ast_nodes.h"
#include <iostream>  ////TODO(PP) Remove
#include "catalog/catalog.h"
#include "codegen/lang/if.h"
#include "codegen/lang/loop.h"
#include "codegen/type/type.h"

namespace peloton {
namespace udf {

std::map<std::string, llvm::Value *> variable_alloc;

// Codegen for SeqStmtAST
void SeqStmtAST::Codegen(codegen::CodeGen &codegen,
                         codegen::FunctionBuilder &fb,
                         UNUSED_ATTRIBUTE codegen::Value *dst) {
  for (uint32_t i = 0; i < stmts.size(); i++) {
    stmts[i]->Codegen(codegen, fb, nullptr);
  }

  return;
}

// Codegen for DeclStmtAST
void DeclStmtAST::Codegen(
    peloton::codegen::CodeGen &codegen,
    UNUSED_ATTRIBUTE peloton::codegen::FunctionBuilder &fb,
    UNUSED_ATTRIBUTE codegen::Value *dst) {
  variable_alloc[name] = codegen.AllocateVariable(codegen.DoubleType(), name);
  return;
}

// Codegen for NumberExprAST
void NumberExprAST::Codegen(
    peloton::codegen::CodeGen &codegen,
    UNUSED_ATTRIBUTE peloton::codegen::FunctionBuilder &fb,
    codegen::Value *dst) {
  *dst = peloton::codegen::Value(
      peloton::codegen::type::Type(type::TypeId::DECIMAL, false),
      codegen.ConstDouble(val));
  return;
}

// Codegen for VariableExprAST
void VariableExprAST::Codegen(
    UNUSED_ATTRIBUTE peloton::codegen::CodeGen &codegen,
    peloton::codegen::FunctionBuilder &fb, codegen::Value *dst) {
  llvm::Value *val = fb.GetArgumentByName(name);

  if (val) {
    *dst = peloton::codegen::Value(
        peloton::codegen::type::Type(type::TypeId::DECIMAL, false), val);
    return;
  } else {
    // Assuming each variable is defined
    *dst = peloton::codegen::Value(
        peloton::codegen::type::Type(type::TypeId::DECIMAL, false),
        codegen->CreateLoad(variable_alloc[name]));
    return;
  }
  return;
}

llvm::Value *VariableExprAST::GetAllocVal() { return variable_alloc[name]; }

// Codegen for BinaryExprAST
void BinaryExprAST::Codegen(codegen::CodeGen &codegen,
                            codegen::FunctionBuilder &fb, codegen::Value *dst) {
  // if (op == '=') {
  //   auto *right_val = (rhs->Codegen(codegen, fb)).GetValue();
  //   // VariableExprAST *var_exp = dynamic_cast<VariableExprAST*>(lhs);
  //   // auto *left_val = lhs->GetAllocVal();
  //   auto *left_val = variable_alloc["x"];
  //   auto *ret_val = codegen->CreateStore(right_val, left_val);
  //   return peloton::codegen::Value(
  //       peloton::codegen::type::Type(type::TypeId::DECIMAL, false), ret_val);
  // }

  codegen::Value left;
  lhs->Codegen(codegen, fb, &left);
  codegen::Value right;
  rhs->Codegen(codegen, fb, &right);
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
                          codegen::FunctionBuilder &fb, codegen::Value *dst) {
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
    args[i]->Codegen(codegen, fb, &arg_val);
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
                        UNUSED_ATTRIBUTE codegen::Value *dst) {
  PL_ASSERT(dst == nullptr);
  auto compare_value = peloton::codegen::Value(
      peloton::codegen::type::Type(type::TypeId::DECIMAL, false),
      codegen.ConstDouble(1.0));

  peloton::codegen::Value cond_expr_value;
  cond_expr->Codegen(codegen, fb, &cond_expr_value);

  // Codegen If condition expression
  codegen::lang::If entry_cond{
      codegen, cond_expr_value.CompareEq(codegen, compare_value), "entry_cond"};
  {
    // Codegen the then statements
    then_stmt->Codegen(codegen, fb, nullptr);
  }
  entry_cond.ElseBlock("multipleValue");
  {
    // codegen the else statements
    else_stmt->Codegen(codegen, fb, nullptr);
  }
  entry_cond.EndIf();

  return;
}

void WhileStmtAST::Codegen(codegen::CodeGen &codegen,
                           codegen::FunctionBuilder &fb, codegen::Value *dst) {
  PL_ASSERT(dst == nullptr);
  // TODO(boweic): Use boolean when supported
  auto compare_value =
      peloton::codegen::Value(codegen::type::Type(type::TypeId::DECIMAL, false),
                              codegen.ConstDouble(1.0));

  peloton::codegen::Value cond_expr_value;
  cond_expr->Codegen(codegen, fb, &cond_expr_value);

  // Codegen If condition expression
  codegen::lang::Loop loop{
      codegen,
      cond_expr_value.CompareEq(codegen, compare_value).GetValue(),
      {}};
  {
    body_stmt->Codegen(codegen, fb, nullptr);
    // TODO(boweic): Use boolean when supported
    auto compare_value = peloton::codegen::Value(
        codegen::type::Type(type::TypeId::DECIMAL, false),
        codegen.ConstDouble(1.0));

    peloton::codegen::Value cond_expr_value;
    cond_expr->Codegen(codegen, fb, &cond_expr_value);
    loop.LoopEnd(cond_expr_value.CompareEq(codegen, compare_value).GetValue(),
                 {});
  }

  return;
}
// Codegen for RetStmtAST
void RetStmtAST::Codegen(codegen::CodeGen &codegen,
                         UNUSED_ATTRIBUTE codegen::FunctionBuilder &fb,
                         UNUSED_ATTRIBUTE codegen::Value *dst) {
  peloton::codegen::Value decl_codegen_val;
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
    expr->Codegen(codegen, fb, &expr_ret_val);
    codegen->CreateRet(expr_ret_val.GetValue());
  }
  return;
}

// Codegen for AssignStmtAST
void AssignStmtAST::Codegen(codegen::CodeGen &codegen,
                            UNUSED_ATTRIBUTE codegen::FunctionBuilder &fb,
                            UNUSED_ATTRIBUTE codegen::Value *dst) {
  codegen::Value right_val;
  rhs->Codegen(codegen, fb, &right_val);
  auto *left_val = lhs->GetAllocVal();
  codegen->CreateStore(right_val.GetValue(), left_val);
}

// Codegen for FunctionAST
llvm::Function *FunctionAST::Codegen(peloton::codegen::CodeGen &codegen,
                                     peloton::codegen::FunctionBuilder &fb) {
  variable_alloc.clear();
  body->Codegen(codegen, fb, nullptr);
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
