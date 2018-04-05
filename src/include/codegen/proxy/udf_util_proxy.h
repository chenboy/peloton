//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// udf_util_proxy.h
//
// Identification: src/include/codegen/proxy/udf_util_proxy.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/proxy/proxy.h"
#include "codegen/proxy/type_builder.h"
#include "udf/udf_util.h"

namespace peloton {
namespace codegen {

PROXY(UDFUtil) { DECLARE_METHOD(ExecuteSQLHelper); };

}  // namespace codegen
}  // namespace peloton