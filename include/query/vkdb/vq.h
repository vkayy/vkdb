#ifndef QUERY_VQ_H
#define QUERY_VQ_H

#include <vkdb/token.h>
#include <filesystem>
#include <cstdint>

namespace vkdb {
class Database;
class Interpreter;
class RuntimeError;

/**
 * @brief Standalone vq interpreter.
 * 
 */
class VQ {
public:
  using size_type = uint64_t;
  
  /**
   * @brief Deleted default constructor.
   * 
   */
  VQ() = default;

  /**
   * @brief Move-construct a VQ object.
   * 
   */
  VQ(VQ&&) noexcept = default;

  /**
   * @brief Move-assign a VQ object.
   * 
   */
  VQ& operator=(VQ&&) noexcept = default;

  /**
   * @brief Copy-construct a VQ object.
   * 
   */
  VQ(const VQ&) noexcept = default;

  /**
   * @brief Copy-assign a VQ object.
   * 
   */
  VQ& operator=(const VQ&) noexcept = default;

  /**
   * @brief Destroy the VQ object.
   * 
   */
  ~VQ() noexcept = default;

  /**
   * @brief Run a file.
   * 
   * @param path Path to the file.
   */
  static void runFile(const std::filesystem::path path) noexcept;

  /**
   * @brief Run the prompt.
   * @details The user can enter queries and commands interactively.
   * 
   */
  static void runPrompt() noexcept;

  /**
   * @brief Run a source string.
   * 
   * @param source Source string.
   */
  static void run(const std::string& source) noexcept;

  /**
   * @brief Handle an error.
   * @details Reports the error and sets the had_error_ flag.
   * 
   * @param token Token where the error occurred.
   * @param message Error message.
   */
  static void error(Token token, const std::string& message) noexcept;

  /**
   * @brief Handle a runtime error.
   * @details Reports the runtime error and sets the had_runtime_error_ flag.
   * 
   * @param error Runtime error.
   */
  static void runtimeError(const RuntimeError& error) noexcept;

private:
  /**
   * @brief Report an error.
   * 
   * @param line Line number.
   * @param where Where the error occurred.
   * @param message Error message.
   */
  static void report(
    size_type line,
    const std::string& where,
    const std::string& message
  ) noexcept;

  /**
   * @brief Flag for errors.
   * 
   */
  static bool had_error_;

  /**
   * @brief Flag for runtime errors.
   * 
   */
  static bool had_runtime_error_;

  /**
   * @brief The database.
   * 
   */
  static Database database_;

  /**
   * @brief The interpreter.
   * 
   */
  static const Interpreter interpreter_;
};
}  // namespace vkdb

#endif // QUERY_VQ_H