#include "udf/plpgsql_parser.h"
#include "udf/udf_handler.h"
#include "udf/udf_context.h"

namespace peloton {
namespace udf {

UDFHandler::UDFHandler() {}

std::shared_ptr<codegen::CodeContext> UDFHandler::Execute(
    UNUSED_ATTRIBUTE concurrency::TransactionContext *txn,
    std::string func_name, std::string func_body,
    UNUSED_ATTRIBUTE std::vector<std::string> args_name,
    UNUSED_ATTRIBUTE std::vector<arg_type> args_type,
    UNUSED_ATTRIBUTE arg_type ret_type) {
  return Compile(txn, func_name, func_body, args_name, args_type, ret_type);
}

llvm::Function *UDFHandler::RegisterExternalFunction(
    peloton::codegen::CodeGen &codegen,
    const expression::FunctionExpression &func_expr) {
  // The code_context associated with the UDF
  auto func_context = func_expr.GetFuncContext();

  // Construct the new functionType in this context
  llvm::Type *llvm_ret_type =
      GetCodegenType(func_expr.GetValueType(), codegen);

  // vector of pair of <argument name, argument type>
  std::vector<llvm::Type *> llvm_args;

  auto args_type = func_expr.GetArgTypes();
  auto iterator_arg_type = args_type.begin();

  while (iterator_arg_type != args_type.end()) {
    llvm_args.push_back(GetCodegenType(*iterator_arg_type, codegen));

    ++iterator_arg_type;
  }

  auto *fn_type = llvm::FunctionType::get(llvm_ret_type, llvm_args, false);

  // Construct the function prototype
  auto *func_ptr = llvm::Function::Create(
      fn_type, llvm::Function::ExternalLinkage, func_expr.GetFuncName(),
      &(codegen.GetCodeContext().GetModule()));

  // Register the Function Prototype in this context
  codegen.GetCodeContext().RegisterExternalFunction(
      func_ptr, func_context->GetRawFunctionPointer(func_context->GetUDF()));

  return func_ptr;
}

std::shared_ptr<codegen::CodeContext> UDFHandler::Compile(
    UNUSED_ATTRIBUTE concurrency::TransactionContext *txn,
    std::string func_name, std::string func_body,
    std::vector<std::string> args_name, std::vector<arg_type> args_type,
    arg_type ret_type) {
  // To contain the context of the UDF
  auto code_context = std::make_shared<codegen::CodeContext>();
  // codegen::CodeContext *code_context = new codegen::CodeContext();
  codegen::CodeGen cg{*code_context};

  llvm::Type *llvm_ret_type = GetCodegenType(ret_type, cg);

  // vector of pair of <argument name, argument type>
  std::vector<codegen::FunctionDeclaration::ArgumentInfo> llvm_args;
  UDFContext udf_context(func_name, ret_type, args_type);

  auto iterator_arg_name = args_name.begin();
  auto iterator_arg_type = args_type.begin();

  while (iterator_arg_name != args_name.end() &&
         iterator_arg_type != args_type.end()) {
    udf_context.SetVariableType(*iterator_arg_name, *iterator_arg_type);
    llvm_args.emplace_back(*iterator_arg_name,
                           GetCodegenType(*iterator_arg_type, cg));
    ++iterator_arg_name;
    ++iterator_arg_type;
  }

  // Construct the Function Builder object
  codegen::FunctionBuilder fb{*code_context, func_name, llvm_ret_type,
                              llvm_args};

  // Construct UDF Parser Object
  // std::unique_ptr<UDFParser> parser(new UDFParser(txn));

  // Parse UDF and generate the AST
  // parser->ParseUDF(cg, fb, func_body, func_name, args_type);
  PLpgSQLParser parser(&udf_context);
  LOG_DEBUG("func_body : %s", func_body.c_str());
  auto func = parser.ParsePLpgSQL(func_body);
  LOG_INFO("Parsing successful");
  PL_ASSERT(func != nullptr);
  if (auto *func_ptr = func->Codegen(cg, fb, &udf_context)) {
    // Required for referencing from Peloton code
    code_context->SetUDF(func_ptr);

    // To check correctness of the codegened UDF
    func_ptr->dump();
  }

  // Optimize and JIT compile all functions created in this context
  code_context->Compile();

  return code_context;
}

llvm::Type *UDFHandler::GetCodegenType(type::TypeId type_val,
                                            peloton::codegen::CodeGen &cg) {
  // TODO[Siva]: Add more types later
  switch (type_val) {
    case type::TypeId::INTEGER : {
      return cg.Int32Type();
    } 
    case type::TypeId::DECIMAL : {
      return cg.DoubleType();
    }
    default : {
      throw Exception("UDFHandler : Expression type not supported");
    }
  }
}

}  // namespace udf
}  // namespace peloton
