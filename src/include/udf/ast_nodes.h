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
#include "type/value.h"
#include "udf/ast_node_visitor.h"
#include "udf/udf_context.h"

namespace peloton {
namespace udf {

// AbstractAST - Base class for all AST nodes.
class AbstractAST {
 public:
  virtual ~AbstractAST() = default;
  virtual void Accept(ASTNodeVisitor *visitor) { visitor->Visit(this); };

  // virtual void Codegen(codegen::CodeGen &codegen, codegen::FunctionBuilder &fb,
                       // codegen::Value *dst, UDFContext *udf_context) = 0;
};

// StmtAST - Base class for all statement nodes.
class StmtAST : public AbstractAST {
 public:
  virtual ~StmtAST() = default;
  void Accept(ASTNodeVisitor *visitor) override { visitor->Visit(this); };

  // virtual void Codegen(peloton::codegen::CodeGen &codegen,
                       // peloton::codegen::FunctionBuilder &fb,
                       // codegen::Value *dst,
                       // UDFContext *udf_context) override = 0;
};

// ExprAST - Base class for all expression nodes.
class ExprAST : public StmtAST {
 public:
  virtual ~ExprAST() = default;
  void Accept(ASTNodeVisitor *visitor) override { visitor->Visit(this); };

  // virtual void Codegen(peloton::codegen::CodeGen &codegen,
                       // peloton::codegen::FunctionBuilder &fb,
                       // codegen::Value *dst, UDFContext *udf_context) override = 0;
};

// DoubleExprAST - Expression class for numeric literals like "1.1".
class ValueExprAST : public ExprAST {
 public:
  type::Value value_;
  ValueExprAST(type::Value value) : value_(value) {}

  void Accept(ASTNodeVisitor *visitor) override { visitor->Visit(this); };

  // void Codegen(peloton::codegen::CodeGen &codegen,
               // peloton::codegen::FunctionBuilder &fb, codegen::Value *dst,
               // UDFContext *udf_context) override;
};

// VariableExprAST - Expression class for referencing a variable, like "a".
class VariableExprAST : public ExprAST {
 public:
  std::string name;
  VariableExprAST(const std::string &name) : name(name) {}

  void Accept(ASTNodeVisitor *visitor) override { visitor->Visit(this); };

  // void Codegen(peloton::codegen::CodeGen &codegen,
               // peloton::codegen::FunctionBuilder &fb, codegen::Value *dst,
               // UDFContext *udf_context) override;

  peloton::codegen::Value GetAllocValue(UDFContext *udf_context) {
    return udf_context->GetAllocValue(name);
  }

  type::TypeId GetVarType(UDFContext *udf_context) {
    return udf_context->GetVariableType(name);
  }
};

// BinaryExprAST - Expression class for a binary operator.
class BinaryExprAST : public ExprAST {
 public:
  ExpressionType op;
  std::unique_ptr<ExprAST> lhs, rhs;

  BinaryExprAST(ExpressionType op, std::unique_ptr<ExprAST> lhs,
                std::unique_ptr<ExprAST> rhs)
      : op(op), lhs(std::move(lhs)), rhs(std::move(rhs)) {}

  void Accept(ASTNodeVisitor *visitor) override { visitor->Visit(this); };

  // void Codegen(peloton::codegen::CodeGen &codegen,
               // peloton::codegen::FunctionBuilder &fb, codegen::Value *dst,
               // UDFContext *udf_context) override;
};

// CallExprAST - Expression class for function calls.
class CallExprAST : public ExprAST {
 public:
  std::string callee;
  std::vector<std::unique_ptr<ExprAST>> args;

  CallExprAST(const std::string &callee,
              std::vector<std::unique_ptr<ExprAST>> args)
      : callee(callee), args(std::move(args)) {}

  void Accept(ASTNodeVisitor *visitor) override { visitor->Visit(this); };

  // void Codegen(peloton::codegen::CodeGen &codegen,
               // peloton::codegen::FunctionBuilder &fb, codegen::Value *dst,
               // UDFContext *udf_context) override;
};

// SeqStmtAST - Statement class for sequence of statements
class SeqStmtAST : public StmtAST {
 public:
  std::vector<std::unique_ptr<StmtAST>> stmts;

  SeqStmtAST(std::vector<std::unique_ptr<StmtAST>> stmts)
      : stmts(std::move(stmts)) {}

  void Accept(ASTNodeVisitor *visitor) override { visitor->Visit(this); };

  // void Codegen(peloton::codegen::CodeGen &codegen,
               // peloton::codegen::FunctionBuilder &fb, codegen::Value *dst,
               // UDFContext *udf_context) override;
};

// DeclStmtAST - Statement class for sequence of statements
class DeclStmtAST : public StmtAST {
 public:
  std::string name;
  type::TypeId type;

  DeclStmtAST(std::string name, type::TypeId type)
      : name(std::move(name)), type(std::move(type)) {}

  void Accept(ASTNodeVisitor *visitor) override { visitor->Visit(this); };

  // void Codegen(peloton::codegen::CodeGen &codegen,
               // peloton::codegen::FunctionBuilder &fb, codegen::Value *dst,
               // UDFContext *udf_context) override;
};

// IfStmtAST - Statement class for if/then/else.
class IfStmtAST : public ExprAST {
 public:
  std::unique_ptr<ExprAST> cond_expr;
  std::unique_ptr<StmtAST> then_stmt, else_stmt;

  void Accept(ASTNodeVisitor *visitor) override { visitor->Visit(this); };

  IfStmtAST(std::unique_ptr<ExprAST> cond_expr,
            std::unique_ptr<StmtAST> then_stmt,
            std::unique_ptr<StmtAST> else_stmt)
      : cond_expr(std::move(cond_expr)),
        then_stmt(std::move(then_stmt)),
        else_stmt(std::move(else_stmt)) {}

  // void Codegen(peloton::codegen::CodeGen &codegen,
               // peloton::codegen::FunctionBuilder &fb, codegen::Value *dst,
               // UDFContext *udf_context) override;
};

// WhileAST - Statement class for while loop
class WhileStmtAST : public ExprAST {
 public:
  std::unique_ptr<ExprAST> cond_expr;
  std::unique_ptr<StmtAST> body_stmt;

  void Accept(ASTNodeVisitor *visitor) override { visitor->Visit(this); };

  WhileStmtAST(std::unique_ptr<ExprAST> cond_expr,
               std::unique_ptr<StmtAST> body_stmt)
      : cond_expr(std::move(cond_expr)), body_stmt(std::move(body_stmt)) {}

  // void Codegen(peloton::codegen::CodeGen &codegen,
               // peloton::codegen::FunctionBuilder &fb, codegen::Value *dst,
               // UDFContext *udf_context) override;
};

// RetStmtAST - Statement class for sequence of statements
class RetStmtAST : public StmtAST {
 public:
  std::unique_ptr<ExprAST> expr;

  RetStmtAST(std::unique_ptr<ExprAST> expr) : expr(std::move(expr)) {}

  void Accept(ASTNodeVisitor *visitor) override { visitor->Visit(this); };

  // void Codegen(peloton::codegen::CodeGen &codegen,
               // peloton::codegen::FunctionBuilder &fb, codegen::Value *dst,
               // UDFContext *udf_context) override;
};

// AssignStmtAST - Expression class for a binary operator.
class AssignStmtAST : public ExprAST {
 public:
  std::unique_ptr<VariableExprAST> lhs;
  std::unique_ptr<ExprAST> rhs;

  AssignStmtAST(std::unique_ptr<VariableExprAST> lhs,
                std::unique_ptr<ExprAST> rhs)
      : lhs(std::move(lhs)), rhs(std::move(rhs)) {}

  void Accept(ASTNodeVisitor *visitor) override { visitor->Visit(this); };

  // void Codegen(peloton::codegen::CodeGen &codegen,
               // peloton::codegen::FunctionBuilder &fb, codegen::Value *dst,
               // UDFContext *udf_context) override;
};

// FunctionAST - This class represents a function definition itself.
class FunctionAST {
 public:
  std::unique_ptr<StmtAST> body;

  FunctionAST(std::unique_ptr<StmtAST> body) : body(std::move(body)) {}

  llvm::Function *Codegen(peloton::codegen::CodeGen &codegen,
                          peloton::codegen::FunctionBuilder &fb,
                          UDFContext *udf_context);
};

/*----------------------------------------------------------------
/// Error* - These are little helper functions for error handling.
-----------------------------------------------------------------*/

std::unique_ptr<ExprAST> LogError(const char *str);
peloton::codegen::Value LogErrorV(const char *str);

}  // namespace udf
}  // namespace peloton
