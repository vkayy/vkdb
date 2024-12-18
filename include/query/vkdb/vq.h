#ifndef QUERY_VQ_H
#define QUERY_VQ_H

#include <vkdb/lexer.h>
#include <vkdb/parser.h>
#include <vkdb/interpreter.h>
#include <vkdb/printer.h>
#include <iostream>
#include <filesystem>
#include <fstream>
#include <sstream>

namespace vkdb {
class VQ {
public:
  using size_type = uint64_t;
  
  VQ() = default;

  VQ(VQ&&) noexcept = default;
  VQ& operator=(VQ&&) noexcept = default;

  VQ(const VQ&) noexcept = default;
  VQ& operator=(const VQ&) noexcept = default;

  ~VQ() = default;

  static void runFile(const std::filesystem::path path) {
    std::ifstream file{path};
    if (!file.is_open()) {
      std::cerr << "VQ::runFile(): Could not open file " << path << "\n";
      return;
    }
    std::stringstream buffer;
    buffer << file.rdbuf();
    run(buffer.str());
  }

  static void runPrompt() {
    std::cout << "\033[1;31mwelcome to the vq repl! :)\033[0m\n";
    std::string line;
    while (true) {
      std::cout << "\033[1;34m(vq) >> \033[0m";
      if (!std::getline(std::cin, line) || line.empty()) {
        break;
      };
      run(line);
      had_error_ = false;
    }
  }

  static void run(const std::string& source) {
    Lexer lexer{source};
    auto tokens{lexer.tokenize()};

    Parser parser{tokens, error};
    auto expr{parser.parse()};

    if (had_error_) {
      return;
    }

    interpreter_.interpret(expr.value());
  }

  static void error(Token token, const std::string& message) {
    if (token.type() == TokenType::END_OF_FILE) {
      report(token.line(), "at end", message);
    } else {
      report(token.line(), "at '" + token.lexeme() + "'", message);
    }
  }

  static void runtimeError(const RuntimeError& error) {
    std::cerr << "\033[1;32m[line " << error.token().line();
    std::cerr << "] Runtime error: " << error.message() << "\033[0m\n";
    had_runtime_error_ = true;
  }

private:
  static void report(
    size_type line,
    const std::string& where,
    const std::string& message
  ) {
    std::cerr << "\033[1;32m[line " << line << "] Parse error ";
    std::cerr << where << ": " << message << "\033[0m\n";
    had_error_ = true;
  }

  static bool had_error_;
  static bool had_runtime_error_;
  static Database database_;
  static const Interpreter interpreter_;
};
}  // namespace vkdb

#endif // QUERY_VQ_H