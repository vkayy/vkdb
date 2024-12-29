#ifndef QUERY_LEXER_H
#define QUERY_LEXER_H

#include <vkdb/token.h>
#include <vector>
#include <unordered_map>
#include <concepts>

namespace vkdb {
/**
 * @brief Mapping of reserved words to their corresponding token types.
 * 
 */
static const std::unordered_map<Lexeme, TokenType> WORD_TO_TOKEN_TYPE{
	{"SELECT", TokenType::SELECT},
	{"PUT", TokenType::PUT},
	{"DELETE", TokenType::DELETE},
	{"CREATE", TokenType::CREATE},
	{"DROP", TokenType::DROP},
	{"ADD", TokenType::ADD},
	{"REMOVE", TokenType::REMOVE},
	{"TABLES", TokenType::TABLES},
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

  /**
   * @brief Deleted default constructor.
   * 
   */
	Lexer() = delete;

  /**
   * @brief Construct a new Lexer object.
   * 
   * @param input The input string to tokenize.
   */
	explicit Lexer(const std::string& input) noexcept;

  /**
   * @brief Move-construct a new Lexer object.
   * 
   */
	Lexer(Lexer&&) noexcept = default;

  /**
   * @brief Move-assign a new Lexer object.
   * 
   */
	Lexer& operator=(Lexer&&) noexcept = default;

  /**
   * @brief Deleted copy constructor.
   * 
   */
	Lexer(const Lexer&) = delete;

  /**
   * @brief Deleted copy assignment operator.
   * 
   */
	Lexer& operator=(const Lexer&) = delete;

  /**
   * @brief Destroy the Lexer object.
   * 
   */
	~Lexer() noexcept = default;

  /**
   * @brief Tokenize the input string.
   * 
   * @return std::vector<Token> The vector of tokens.
   * 
   * @throw std::exception If tokenization fails.
   */
	std::vector<Token> tokenize();

private:
  /**
   * @brief Check if a character is whitespace.
   * 
   * @param ch The character to check.
   * @return true if the character is whitespace.
   * @return false if the character is not whitespace.
   */
	[[nodiscard]] bool is_whitespace(char ch) const noexcept;

  /**
   * @brief Check if a character is alphabetic.
   * @details Alphabetic characters are the underscore, lowercase, and
   * uppercase letters.
   * 
   * @param ch The character to check.
   * @return true if the character is alphabetic.
   * @return false if the character is not alphabetic.
   */
	[[nodiscard]] bool is_alpha(char ch) const noexcept;

  /**
   * @brief Check if a character is a digit.
   * 
   * @param ch The character to check.
   * @return true if the character is a digit.
   * @return false if the character is not a digit.
   */
  [[nodiscard]] bool is_digit(char ch) const noexcept;

  /**
   * @brief Check if a character is alphanumeric.
   * 
   * @param ch The character to check.
   * @return true if the character is alphanumeric.
   * @return false if the character is not alphanumeric.
   */
  [[nodiscard]] bool is_alnum(char ch) const noexcept;

  /**
   * @brief Check if there are characters remaining to tokenize.
   * @details Checks whether position is less than the length of the input.
   * 
   * @return true if there are characters remaining.
   * @return false if there are no characters remaining.
   */
  [[nodiscard]] bool chars_remaining() const noexcept;

  /**
   * @brief Peek at the current character.
   * 
   * @return char The current character.
   * 
   * @throw std::out_of_range If there are no characters remaining.
   */
  [[nodiscard]] char peek() const;

  /**
   * @brief Peek at the next character.
   * 
   * @return char The next character.
   * 
   * @throw std::out_of_range If there are no characters remaining.
   */
  [[nodiscard]] char peek_next() const;

  /**
   * @brief Advance the lexer to the next character.
   * 
   * @return char The current character.
   * 
   * @throw std::out_of_range If there are no characters remaining.
   */
	char advance();

  /**
   * @brief Advance the lexer while a predicate is true.
   * 
   * @tparam Pred The predicate type.
   * @param pred The predicate to check.
   */
	template <std::predicate<char> Pred>
	void advance_while(const Pred& pred) noexcept;

  /**
   * @brief Lex whitespace.
   * @details Advances the lexer while the current character is whitespace.
   * 
   */
	void lex_whitespace() noexcept;

  /**
   * @brief Lex a word.
   * @details Advances the lexer while the current character is alphanumeric.
   * 
   * @return Token The token representing the word.
   */
	[[nodiscard]] Token lex_word() noexcept;

  /**
   * @brief Lex a number.
   * @details Checks if the number is negative, advances the lexer while the
   * current character is a digit, and advances the lexer if the number is a
   * decimal.
   * 
   * @return Token The token representing the number.
   */
	[[nodiscard]] Token lex_number() noexcept;

  /**
   * @brief Lex a comment.
   * @details Advances the lexer while the current character is not a newline.
   * 
   */
	void lex_comment() noexcept;

  /**
   * @brief Lex an equal sign.
   * 
   * @return Token The token representing the equal sign.
   */
	[[nodiscard]] Token lex_equal() noexcept;

  /**
   * @brief Lex a comma.
   * 
   * @return Token The token representing the comma.
   */
	[[nodiscard]] Token lex_comma() noexcept;

  /**
   * @brief Lex a semicolon.
   * 
   * @return Token The token representing the semicolon.
   */
	[[nodiscard]] Token lex_semicolon() noexcept;

  /**
   * @brief Lex an unknown character.
   * 
   * @return Token The token representing the unknown character.
   */
	[[nodiscard]] Token lex_unknown() noexcept;

  /**
   * @brief Lex the end of the file.
   * 
   * @return Token The token representing the end of the file.
   */
	[[nodiscard]] Token lex_end_of_file() noexcept;

  /**
   * @brief Make a lexeme from a starting position.
   * 
   * @param start The starting position.
   * @return Lexeme The lexeme.
   * 
   * @throw std::exception If the substring is out of range.
   */
	[[nodiscard]] Lexeme make_lexeme_from(size_type start) const;

  /**
   * @brief Make a token from a token type and lexeme.
   * 
   * @param type The token type.
   * @param lexeme The lexeme.
   * @return Token The token.
   */
	[[nodiscard]] Token make_token(
    TokenType type,
    const Lexeme& lexeme
  ) noexcept;

  /**
   * @brief The input string.
   * 
   */
	std::string input_;

  /**
   * @brief The current position in the input string.
   * 
   */
	size_type position_;

  /**
   * @brief The current line number.
   * 
   */
	size_type line_;

  /**
   * @brief The current column number.
   * 
   */
	size_type column_;
};

}  // namespace vkdb

#endif // QUERY_LEXER_H