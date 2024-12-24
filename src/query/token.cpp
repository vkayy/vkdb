#include <vkdb/token.h>

namespace vkdb {
Token::Token(TokenType type, const Lexeme& lexeme, Token::size_type line,
      Token::size_type column) noexcept
  : type_{type}, lexeme_{lexeme}, line_{line}, column_{column} {}

bool Token::operator==(const Token& other) const noexcept {
  return type_ == other.type() &&
    lexeme_ == other.lexeme() &&
    line_ == other.line() &&
    column_ == other.column();
}

TokenType Token::type() const noexcept {
  return type_;
}

Lexeme Token::lexeme() const noexcept {
  return lexeme_;
}

Token::size_type Token::line() const noexcept {
  return line_;
}

Token::size_type Token::column() const noexcept {
  return column_;
}

std::string Token::str() const noexcept {
  auto type_str{TOKEN_TYPE_TO_STRING.at(type_)};
  return type_str + " "
    + lexeme_ + " "
    + std::to_string(line_) + " "
    + std::to_string(column_);
}
}  // namespace vkdb
