#ifndef QUERY_LEXER_H
#define QUERY_LEXER_H

#include <vkdb/token.h>
#include <vector>
#include <unordered_map>
#include <concepts>

namespace vkdb {
static const std::unordered_map<Lexeme, TokenType> WORD_TO_TOKEN_TYPE{
  {"SELECT", TokenType::SELECT},
  {"PUT", TokenType::PUT},
  {"DELETE", TokenType::DELETE},
  {"CREATE", TokenType::CREATE},
  {"DROP", TokenType::DROP},
  {"ADD", TokenType::ADD},
  {"REMOVE", TokenType::REMOVE},
  {"DATA", TokenType::DATA},
  {"AVG", TokenType::AVG},
  {"SUM", TokenType::SUM},
  {"COUNT", TokenType::COUNT},
  {"MIN", TokenType::MIN},
  {"MAX", TokenType::MAX},
  {"TABLE", TokenType::TABLE},
  {"TAGS", TokenType::TAGS},
  {"ALL", TokenType::ALL},
  {"BETWEEN", TokenType::BETWEEN},
  {"AND", TokenType::AND},
  {"AT", TokenType::AT},
  {"WHERE", TokenType::WHERE},
  {"FROM", TokenType::FROM},
  {"INTO", TokenType::INTO},
  {"TO", TokenType::TO}
};

class Lexer {
public:
  using size_type = uint64_t;

  Lexer() = delete;

  explicit Lexer(const std::string& input)
    : input_{input}, position_{0}, line_{1}, column_{1} {}
  
  Lexer(Lexer&&) noexcept = default;
  Lexer& operator=(Lexer&&) noexcept = default;

  Lexer(const Lexer&) = delete;
  Lexer& operator=(const Lexer&) = delete;

  ~Lexer() = default;
  
  std::vector<Token> tokenize() {
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

private:
  [[nodiscard]] bool is_whitespace(char ch) const noexcept {
    return std::isspace(ch);
  }

  [[nodiscard]] bool is_alpha(char ch) const noexcept {
    return std::isalpha(ch) || ch == '_';
  }

  [[nodiscard]] bool is_digit(char ch) const noexcept {
    return std::isdigit(ch);
  }

  [[nodiscard]] bool is_alnum(char ch) const noexcept {
    return is_alpha(ch) || is_digit(ch);
  }

  [[nodiscard]] bool chars_remaining() const noexcept {
    return position_ < input_.length();
  }

  [[nodiscard]] char peek() const {
    return input_[position_];
  }

  [[nodiscard]] char peek_next() const {
    if (position_ + 1 >= input_.length()) {
      return '\0';
    }
    return input_[position_ + 1];
  }

  char advance() {
    auto current{input_[position_++]};
    ++column_;
    if (current == '\n') {
      ++line_;
      column_ = 1;
    }
    return current;
  }
  
  template <typename Pred>
  void advance_while(const Pred& pred) {
    while (chars_remaining() && pred(peek())) {
      advance();
    }
  }

  void lex_whitespace() noexcept {
    advance_while([this](auto ch) { return is_whitespace(ch); });
  }

  [[nodiscard]] Token lex_word() noexcept {
    auto start{position_};
    advance_while([this](auto ch) { return is_alnum(ch); });
    auto lexeme{make_lexeme_from(start)};
    if (WORD_TO_TOKEN_TYPE.contains(lexeme)) {
      return make_token(WORD_TO_TOKEN_TYPE.at(lexeme), lexeme);
    }
    return make_token(TokenType::IDENTIFIER, lexeme);
  }

  [[nodiscard]] Token lex_number() {
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

  void lex_comment() {
    advance_while([this](auto ch) { return ch != '\n'; });
  }

  [[nodiscard]] Token lex_equal() {
    advance();
    return make_token(TokenType::EQUAL, "=");
  }

  [[nodiscard]] Token lex_comma() {
    advance();
    return make_token(TokenType::COMMA, ",");
  }

  [[nodiscard]] Token lex_semicolon() {
    advance();
    return make_token(TokenType::SEMICOLON, ";");
  }

  [[nodiscard]] Token lex_unknown() {
    auto unknown{peek()};
    advance();
    return make_token(TokenType::UNKNOWN, {unknown});
  }

  [[nodiscard]] Token lex_end_of_file() {
    return make_token(TokenType::END_OF_FILE, "");
  }

  [[nodiscard]] Lexeme make_lexeme_from(size_type start) const {
    return input_.substr(start, position_ - start);
  }

  [[nodiscard]] Token make_token(TokenType type, const Lexeme& lexeme) {
    return Token{type, lexeme, line_, column_ - lexeme.length()};
  }

  std::string input_;
  size_type position_;
  size_type line_;
  size_type column_;
};
}  // namespace vkdb

#endif // QUERY_LEXER_H