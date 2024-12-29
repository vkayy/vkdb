#include <vkdb/vq.h>
#include <vkdb/database.h>
#include <vkdb/lexer.h>
#include <vkdb/parser.h>
#include <vkdb/interpreter.h>

namespace vkdb {
bool VQ::had_error_ = false;
bool VQ::had_runtime_error_ = false;
Database VQ::database_{INTERPRETER_DEFAULT_DATABASE};
const Interpreter VQ::interpreter_{database_, VQ::runtimeError};

void VQ::runFile(const std::filesystem::path path) noexcept {
  if (path.extension() != ".vq") {
    std::cerr << "\033[1;32mVQ::runFile(): File extension cannot be "
    << path.extension() << ", must be .vq.\033[0m\n";
    return;
  }
  std::ifstream file{path};
  if (!file.is_open()) {
    std::cerr << "\033[1;32mVQ::runFile(): Unable to open file "
    << path << ".\033[0m\n";
    return;
  }
  std::stringstream buffer;
  buffer << file.rdbuf();
  run(buffer.str());
}

void VQ::runPrompt() noexcept {
  std::cout << "\033[1;31mwelcome to the vq repl! :)\033[0m\n";
  std::cout << "\033[1;31m(on default interpreter database)\033[0m\n";
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

void VQ::run(const std::string& source) noexcept {
  Lexer lexer{source};
  auto tokens{lexer.tokenize()};

  Parser parser{tokens, error};
  auto expr{parser.parse()};

  if (had_error_) {
    return;
  }

  interpreter_.interpret(expr.value());
}

void VQ::error(Token token, const std::string& message) noexcept {
  if (token.type() == TokenType::END_OF_FILE) {
    report(token.line(), "at end", message);
  } else {
    report(token.line(), "at '" + token.lexeme() + "'", message);
  }
}

void VQ::runtimeError(const RuntimeError& error) noexcept {
  std::cerr << "\033[1;32m[line " << error.token().line();
  std::cerr << "] Runtime error: " << error.message() << "\033[0m\n";
  had_runtime_error_ = true;
}

void VQ::report(
  size_type line,
  const std::string& where,
  const std::string& message
) noexcept {
  std::cerr << "\033[1;32m[line " << line << "] Parse error ";
  std::cerr << where << ": " << message << "\033[0m\n";
  had_error_ = true;
}
}  // namespace vkdb