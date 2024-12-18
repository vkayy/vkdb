#ifndef QUERY_VQ_H
#define QUERY_VQ_H

#include <vkdb/token.h>
#include <filesystem>
#include <cstdint>

namespace vkdb {
class Database;
class Interpreter;
class RuntimeError;

class VQ {
public:
  using size_type = uint64_t;
  
  VQ() = default;

  VQ(VQ&&) noexcept = default;
  VQ& operator=(VQ&&) noexcept = default;

  VQ(const VQ&) noexcept = default;
  VQ& operator=(const VQ&) noexcept = default;

  ~VQ() = default;

  static void runFile(const std::filesystem::path path);
  static void runPrompt();
  static void run(const std::string& source);
  static void error(Token token, const std::string& message);
  static void runtimeError(const RuntimeError& error);

private:
  static void report(
    size_type line,
    const std::string& where,
    const std::string& message
  );

  static bool had_error_;
  static bool had_runtime_error_;
  static Database database_;
  static const Interpreter interpreter_;
};
}  // namespace vkdb

#endif // QUERY_VQ_H