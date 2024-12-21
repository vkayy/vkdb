#include <vkdb/lexer.h>

namespace vkdb {
Lexer::Lexer(const std::string& input) noexcept
  : input_{input}, position_{0}, line_{1}, column_{1} {}

std::vector<Token> Lexer::tokenize() {
  std::vector<Token> tokens;
  while (chars_remaining()) {
    if (is_whitespace(peek())) {
      lex_whitespace();
    } else if (is_alpha(peek())) {
      tokens.push_back(lex_word());
    } else if (is_digit(peek()) || peek() == '-' && is_digit(peek_next())) {
      tokens.push_back(lex_number());
    } else if (peek() == '/' && peek_next() == '/') {
      advance_while([this](auto ch) { return ch != '\n'; });
    } else if (peek() == '=') {
      tokens.push_back(lex_equal());
    } else if (peek() == ',') {
      tokens.push_back(lex_comma());
    } else if (peek() == ';') {
      tokens.push_back(lex_semicolon());
    } else {
      tokens.push_back(lex_unknown());
    }
  }
  tokens.push_back(lex_end_of_file());
  return tokens;
}

bool Lexer::is_whitespace(char ch) const noexcept {
  return std::isspace(ch);
}

bool Lexer::is_alpha(char ch) const noexcept {
  return std::isalpha(ch) || ch == '_';
}

bool Lexer::is_digit(char ch) const noexcept {
  return std::isdigit(ch);
}

bool Lexer::is_alnum(char ch) const noexcept {
  return is_alpha(ch) || is_digit(ch);
}

bool Lexer::chars_remaining() const noexcept {
  return position_ < input_.length();
}

char Lexer::peek() const {
  return input_[position_];
}

char Lexer::peek_next() const {
  if (position_ + 1 >= input_.length()) {
    return '\0';
  }
  return input_[position_ + 1];
}

char Lexer::advance() {
  auto current{input_[position_++]};
  ++column_;
  if (current == '\n') {
    ++line_;
    column_ = 1;
  }
  return current;
}

template <std::predicate<char> Pred>
void Lexer::advance_while(const Pred& pred) {
  while (chars_remaining() && pred(peek())) {
    advance();
  }
}

void Lexer::lex_whitespace() noexcept {
  advance_while([this](auto ch) { return is_whitespace(ch); });
}

Token Lexer::lex_word() noexcept {
  auto start{position_};
  advance_while([this](auto ch) { return is_alnum(ch); });
  auto lexeme{make_lexeme_from(start)};
  if (WORD_TO_TOKEN_TYPE.contains(lexeme)) {
    return make_token(WORD_TO_TOKEN_TYPE.at(lexeme), lexeme);
  }
  return make_token(TokenType::IDENTIFIER, lexeme);
}

Token Lexer::lex_number() noexcept {
  auto start{position_};
  if (peek() == '-') {
    advance();
  }
  advance_while([this](auto ch) { return is_digit(ch); });
  if (peek() == '.' && is_digit(peek_next())) {
    advance();
    advance_while([this](auto ch) { return is_digit(ch); });
  }
  return make_token(TokenType::NUMBER, make_lexeme_from(start));
}

void Lexer::lex_comment() noexcept {
  advance_while([this](auto ch) { return ch != '\n'; });
}

Token Lexer::lex_equal() noexcept {
  advance();
  return make_token(TokenType::EQUAL, "=");
}

Token Lexer::lex_comma() noexcept {
  advance();
  return make_token(TokenType::COMMA, ",");
}

Token Lexer::lex_semicolon() noexcept {
  advance();
  return make_token(TokenType::SEMICOLON, ";");
}

Token Lexer::lex_unknown() noexcept {
  auto unknown{peek()};
  advance();
  return make_token(TokenType::UNKNOWN, {unknown});
}

Token Lexer::lex_end_of_file() noexcept {
  return make_token(TokenType::END_OF_FILE, "");
}

Lexeme Lexer::make_lexeme_from(size_type start) const {
  return input_.substr(start, position_ - start);
}

Token Lexer::make_token(TokenType type, const Lexeme& lexeme) noexcept {
  return Token{type, lexeme, line_, column_ - lexeme.length()};
}

}  // namespace vkdb
