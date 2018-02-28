#include "udf/udf_code_generator.h"

#include "catalog/catalog.h"
#include "codegen/lang/if.h"
#include "codegen/lang/loop.h"
#include "codegen/type/decimal_type.h"
#include "codegen/type/integer_type.h"
#include "codegen/type/type.h"
#include "codegen/value.h"
#include "udf/ast_nodes.h"
#include "udf/util.h"

namespace peloton {
namespace udf {
UDFCodeGenerator::UDFCodeGenerator(codegen::CodeGen *codegen,
                                   codegen::FunctionBuilder *fb,
                                   UDFContext *udf_context)
    : codegen_(codegen), fb_(fb), udf_context_(udf_context), dst_(nullptr){};

void UDFCodeGenerator::GenerateUDF(AbstractAST *ast) { ast->Accept(this); }

void UDFCodeGenerator::Visit(ValueExprAST *ast) {
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
    default:
      throw Exception("ValueExprAST::Codegen : Expression type not supported");
  }
}

void UDFCodeGenerator::Visit(VariableExprAST *ast) {
  llvm::Value *val = fb_->GetArgumentByName(ast->name);
  type::TypeId type = udf_context_->GetVariableType(ast->name);
  // TODO[Siva]: Support Integers for arguments as well
  if (val) {
    *dst_ = codegen::Value(codegen::type::Type(type, false), val);
    return;
  } else {
    // Assuming each variable is defined
    *dst_ = codegen::Value(
        codegen::type::Type(type, false),
        (*codegen_)->CreateLoad(udf_context_->GetAllocValue(ast->name)));
    return;
  }
}
void UDFCodeGenerator::Visit(BinaryExprAST *ast) {
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

void UDFCodeGenerator::Visit(CallExprAST *ast) {
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

void UDFCodeGenerator::Visit(SeqStmtAST *ast) {
  for (uint32_t i = 0; i < ast->stmts.size(); i++) {
    // If already return in the current block, don't continue to generate
    if (codegen_->IsTerminated()) {
      break;
    }
    ast->stmts[i]->Accept(this);
  }
}

void UDFCodeGenerator::Visit(DeclStmtAST *ast) {
  switch (ast->type) {
    // TODO[Siva]: Replace with this a function that returns llvm::Type from
    // type::TypeID
    case type::TypeId::INTEGER: {
      // TODO[Siva]: 32 / 64 bit handling??
      udf_context_->SetAllocValue(
          ast->name,
          codegen_->AllocateVariable(codegen_->Int32Type(), ast->name));
      break;
    }
    case type::TypeId::DECIMAL: {
      udf_context_->SetAllocValue(
          ast->name,
          codegen_->AllocateVariable(codegen_->DoubleType(), ast->name));
      break;
    }
    default: {
      // TODO[Siva]: Should throw an excpetion, but need to figure out "found"
      // and other internal types first.
    }
  }
}

void UDFCodeGenerator::Visit(IfStmtAST *ast) {
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

void UDFCodeGenerator::Visit(WhileStmtAST *ast) {
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

void UDFCodeGenerator::Visit(RetStmtAST *ast) {
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
          codegen::type::Type(udf_context_->GetFunctionReturnType(), false));
    }

    (*codegen_)->CreateRet(expr_ret_val.GetValue());
  }
}

void UDFCodeGenerator::Visit(AssignStmtAST *ast) {
  codegen::Value right_codegen_val;
  dst_ = &right_codegen_val;
  ast->rhs->Accept(this);
  auto *left_val = ast->lhs->GetAllocValue(udf_context_);
  auto right_type = right_codegen_val.GetType();
  auto left_type =
      codegen::type::Type(ast->lhs->GetVarType(udf_context_), false);

  if (right_type != left_type) {
    // TODO[Siva]: Need to check that they can be casted in semantic analysis
    right_codegen_val = right_codegen_val.CastTo(
        *codegen_,
        codegen::type::Type(ast->lhs->GetVarType(udf_context_), false));
  }

  (*codegen_)->CreateStore(right_codegen_val.GetValue(), left_val);
}

}  // namespace udf
}  // namespace peloton
