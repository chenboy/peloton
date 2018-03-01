#include "udf/util.h"

namespace peloton {

namespace udf {
llvm::Type *UDFUtil::GetCodegenType(type::TypeId type_val,
                                    peloton::codegen::CodeGen &cg) {
  // TODO[Siva]: Add more types later
  switch (type_val) {
    case type::TypeId::INTEGER: {
      return cg.Int32Type();
    }
    case type::TypeId::DECIMAL: {
      return cg.DoubleType();
    }
    case type::TypeId::VARCHAR: {
      return cg.CharPtrType();
    }
    default: { throw Exception("UDFHandler : Expression type not supported"); }
  }
}
}
}
