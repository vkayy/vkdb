#include <vkdb/vq.h>

namespace vkdb {
bool VQ::had_error_{false};
bool VQ::had_runtime_error_{false};
Database VQ::database_{INTERPRETER_DEFAULT_DATABASE};
const Interpreter VQ::interpreter_{database_, VQ::runtimeError};
}  // namespace vkdb