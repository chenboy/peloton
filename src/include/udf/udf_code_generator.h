#pragma once
#include "udf/ast_node_visitor.h"

#include "codegen/codegen.h"
#include "codegen/function_builder.h"
#include "codegen/value.h"

namespace peloton {

namespace udf {
class AbstractAST;
class StmtAST;
class ExprAST;
class ValueExprAST;
class VariableExprAST;
class BinaryExprAST;
class CallExprAST;
class SeqStmtAST;
class DeclStmtAST;
class IfStmtAST;
class WhileStmtAST;
class RetStmtAST;
class AssignStmtAST;
class UDFContext;

class UDFCodeGenerator : public ASTNodeVisitor {
 public:
  UDFCodeGenerator(codegen::CodeGen *codegen, codegen::FunctionBuilder *fb,
                   UDFContext *udf_Context);

  void GenerateUDF(AbstractAST *);
  void Visit(ValueExprAST *) override;
  void Visit(VariableExprAST *) override;
  void Visit(BinaryExprAST *) override;
  void Visit(CallExprAST *) override;
  void Visit(SeqStmtAST *) override;
  void Visit(DeclStmtAST *) override;
  void Visit(IfStmtAST *) override;
  void Visit(WhileStmtAST *) override;
  void Visit(RetStmtAST *) override;
  void Visit(AssignStmtAST *) override;

 private:
  codegen::CodeGen *codegen_;
  codegen::FunctionBuilder *fb_;
  UDFContext *udf_context_;
  codegen::Value *dst_;
};
}  // namespace udf
}  // namespace peloton
