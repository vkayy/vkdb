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

	Lexer() = delete;
	explicit Lexer(const std::string& input) noexcept;

	Lexer(Lexer&&) noexcept = default;
	Lexer& operator=(Lexer&&) noexcept = default;

	Lexer(const Lexer&) = delete;
	Lexer& operator=(const Lexer&) = delete;

	~Lexer() = default;

	std::vector<Token> tokenize();

private:
	[[nodiscard]] bool is_whitespace(char ch) const noexcept;
	[[nodiscard]] bool is_alpha(char ch) const noexcept;
	[[nodiscard]] bool is_digit(char ch) const noexcept;
	[[nodiscard]] bool is_alnum(char ch) const noexcept;
	[[nodiscard]] bool chars_remaining() const noexcept;
	[[nodiscard]] char peek() const;
	[[nodiscard]] char peek_next() const;
	char advance();

	template <std::predicate<char> Pred>
	void advance_while(const Pred& pred);

	void lex_whitespace() noexcept;
	[[nodiscard]] Token lex_word() noexcept;
	[[nodiscard]] Token lex_number();
	void lex_comment();
	[[nodiscard]] Token lex_equal();
	[[nodiscard]] Token lex_comma();
	[[nodiscard]] Token lex_semicolon();
	[[nodiscard]] Token lex_unknown();
	[[nodiscard]] Token lex_end_of_file();

	[[nodiscard]] Lexeme make_lexeme_from(size_type start) const;
	[[nodiscard]] Token make_token(TokenType type, const Lexeme& lexeme);

	std::string input_;
	size_type position_;
	size_type line_;
	size_type column_;
};

}  // namespace vkdb

#endif // QUERY_LEXER_H