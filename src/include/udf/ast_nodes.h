#pragma once

#include "codegen/code_context.h"
#include "codegen/codegen.h"
#include "codegen/function_builder.h"
#include "codegen/value.h"
#include "llvm/ADT/STLExtras.h"
#include "llvm/IR/DerivedTypes.h"
#include "llvm/IR/IRBuilder.h"
#include "llvm/IR/LLVMContext.h"
#include "llvm/IR/Module.h"
#include "llvm/IR/Verifier.h"
#include "llvm/Support/raw_ostream.h"  // For errs()
#include "type/type.h"

namespace peloton {
namespace udf {
// TODO(boweic): naming convention
using arg_type = type::TypeId;

// AbstractAST - Base class for all AST nodes.
class AbstractAST {
 public:
  virtual ~AbstractAST() = default;

  virtual void Codegen(codegen::CodeGen &codegen, codegen::FunctionBuilder &fb,
                       codegen::Value *dst) = 0;
};

// StmtAST - Base class for all statement nodes.
class StmtAST : public AbstractAST {
 public:
  virtual ~StmtAST() = default;

  virtual void Codegen(
      peloton::codegen::CodeGen &codegen,
      peloton::codegen::FunctionBuilder &fb, codegen::Value *dst) = 0;
};

// ExprAST - Base class for all expression nodes.
class ExprAST : public StmtAST {
 public:
  virtual ~ExprAST() = default;

  virtual void Codegen(
      peloton::codegen::CodeGen &codegen,
      peloton::codegen::FunctionBuilder &fb, codegen::Value *dst) = 0;
};

// IntegerExprAST - Expression class for numeric literals like "1".
class IntegerExprAST : public ExprAST {
  // TODO[Siva]: Codegen supports only unsigned int?
  uint64_t val;

 public:
  IntegerExprAST(uint64_t val) : val(val) {}

  void Codegen(
      peloton::codegen::CodeGen &codegen,
      peloton::codegen::FunctionBuilder &fb, codegen::Value *dst) override;
};

// DoubleExprAST - Expression class for numeric literals like "1.1".
class DoubleExprAST : public ExprAST {
  double val;

 public:
  DoubleExprAST(double val) : val(val) {}

  void Codegen(
      peloton::codegen::CodeGen &codegen,
      peloton::codegen::FunctionBuilder &fb, codegen::Value *dst) override;
};

// VariableExprAST - Expression class for referencing a variable, like "a".
class VariableExprAST : public ExprAST {
  std::string name;

 public:
  VariableExprAST(const std::string &name) : name(name) {}

  void Codegen(
      peloton::codegen::CodeGen &codegen,
      peloton::codegen::FunctionBuilder &fb, codegen::Value *dst) override;

  llvm::Value *GetAllocVal();

  type::TypeId GetVarType();
};

// BinaryExprAST - Expression class for a binary operator.
class BinaryExprAST : public ExprAST {
  char op;
  std::unique_ptr<ExprAST> lhs, rhs;

 public:
  BinaryExprAST(char op, std::unique_ptr<ExprAST> lhs,
                std::unique_ptr<ExprAST> rhs)
      : op(op), lhs(std::move(lhs)), rhs(std::move(rhs)) {}

  void Codegen(
      peloton::codegen::CodeGen &codegen,
      peloton::codegen::FunctionBuilder &fb, codegen::Value *dst) override;
};

// CallExprAST - Expression class for function calls.
class CallExprAST : public ExprAST {
  std::string callee;
  std::vector<std::unique_ptr<ExprAST>> args;
  std::string current_func;
  std::vector<arg_type> args_type;

 public:
  CallExprAST(const std::string &callee,
              std::vector<std::unique_ptr<ExprAST>> args,
              std::string &current_func, std::vector<arg_type> args_type)
      : callee(callee), args(std::move(args)) {
    current_func = current_func;
    args_type = args_type;
  }

  void Codegen(
      peloton::codegen::CodeGen &codegen,
      peloton::codegen::FunctionBuilder &fb, codegen::Value *dst) override;
};

// SeqStmtAST - Statement class for sequence of statements
class SeqStmtAST : public StmtAST {
  std::vector<std::unique_ptr<StmtAST>> stmts;

 public:
  SeqStmtAST(std::vector<std::unique_ptr<StmtAST>> stmts)
      : stmts(std::move(stmts)) {}

  void Codegen(
      peloton::codegen::CodeGen &codegen,
      peloton::codegen::FunctionBuilder &fb, codegen::Value *dst) override;
};

// DeclStmtAST - Statement class for sequence of statements
class DeclStmtAST : public StmtAST {
  std::string name;
  type::TypeId type;

 public:
  DeclStmtAST(std::string name, type::TypeId type)
      : name(std::move(name)), type(std::move(type)) {}

  void Codegen(
      peloton::codegen::CodeGen &codegen,
      peloton::codegen::FunctionBuilder &fb, codegen::Value *dst) override;
};

// IfStmtAST - Statement class for if/then/else.
class IfStmtAST : public ExprAST {
  std::unique_ptr<ExprAST> cond_expr;
  std::unique_ptr<StmtAST> then_stmt, else_stmt;

 public:
  IfStmtAST(std::unique_ptr<ExprAST> cond_expr,
            std::unique_ptr<StmtAST> then_stmt,
            std::unique_ptr<StmtAST> else_stmt)
      : cond_expr(std::move(cond_expr)),
        then_stmt(std::move(then_stmt)),
        else_stmt(std::move(else_stmt)) {}

  void Codegen(
      peloton::codegen::CodeGen &codegen,
      peloton::codegen::FunctionBuilder &fb, codegen::Value *dst) override;
};

// RetStmtAST - Statement class for sequence of statements
class RetStmtAST : public StmtAST {
  std::unique_ptr<ExprAST> expr;

 public:
  RetStmtAST(std::unique_ptr<ExprAST> expr) : expr(std::move(expr)) {}

  void Codegen(
      peloton::codegen::CodeGen &codegen,
      peloton::codegen::FunctionBuilder &fb, codegen::Value *dst) override;
};

// AssignStmtAST - Expression class for a binary operator.
class AssignStmtAST : public ExprAST {
  std::unique_ptr<VariableExprAST> lhs;
  std::unique_ptr<ExprAST> rhs;

 public:
  AssignStmtAST(std::unique_ptr<VariableExprAST> lhs,
                std::unique_ptr<ExprAST> rhs)
      : lhs(std::move(lhs)), rhs(std::move(rhs)) {}

  void Codegen(
      peloton::codegen::CodeGen &codegen,
      peloton::codegen::FunctionBuilder &fb, codegen::Value *dst) override;
};


// FunctionAST - This class represents a function definition itself.
class FunctionAST {
  std::unique_ptr<StmtAST> body;

 public:
  FunctionAST(std::unique_ptr<StmtAST> body) : body(std::move(body)) {}

  llvm::Function *Codegen(peloton::codegen::CodeGen &codegen,
                          peloton::codegen::FunctionBuilder &fb);
};

/*----------------------------------------------------------------
/// Error* - These are little helper functions for error handling.
-----------------------------------------------------------------*/

std::unique_ptr<ExprAST> LogError(const char *str);
peloton::codegen::Value LogErrorV(const char *str);

}  // namespace udf
}  // namespace peloton
