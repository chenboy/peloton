//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// udf_util.h
//
// Identification: src/include/udf/udf_util.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/codegen.h"
#include "type/value.h"
namespace peloton {

namespace udf {
class UDFUtil {
 public:
  static llvm::Type *GetCodegenType(type::TypeId type_val,
                                    peloton::codegen::CodeGen &cg);

  static void ExecuteSQLHelper(const char *query, uint32_t len, double *output);
};
}  // namespace udf
}  // namespace peloton
