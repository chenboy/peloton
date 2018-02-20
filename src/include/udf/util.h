#include "type/value.h"
#include "codegen/codegen.h"
namespace peloton {

namespace udf {
class UDFUtil {
 public:
  static llvm::Type *GetCodegenType(type::TypeId type_val,
                                    peloton::codegen::CodeGen &cg);
};
}  // namespace udf
}  // namespace peloton
