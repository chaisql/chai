package scanner

import (
	"strings"
)

// Token is a lexical token of the Genji SQL language.
type Token int

// These are a comprehensive list of Genji SQL language tokens.
const (
	// ILLEGAL Token, EOF, WS are Special Genji SQL tokens.
	ILLEGAL Token = iota
	EOF
	WS
	COMMENT

	literalBeg
	// IDENT and the following are Genji SQL literal tokens.
	IDENT           // main
	NAMEDPARAM      // $param
	POSITIONALPARAM // ?
	NUMBER          // 12345.67
	INTEGER         // 12345
	DURATION        // 13h
	STRING          // "abc"
	BADSTRING       // "abc
	BADESCAPE       // \q
	TRUE            // true
	FALSE           // false
	NULL            // NULL
	REGEX           // Regular expressions
	BADREGEX        // `.*
	literalEnd

	operatorBeg
	// ADD and the following are Genji SQL Operators
	ADD        // +
	SUB        // -
	MUL        // *
	DIV        // /
	MOD        // %
	BITWISEAND // &
	BITWISEOR  // |
	BITWISEXOR // ^

	AND // AND
	OR  // OR

	EQ       // =
	NEQ      // !=
	EQREGEX  // =~
	NEQREGEX // !~
	LT       // <
	LTE      // <=
	GT       // >
	GTE      // >=
	IN       // IN
	IS       // IS
	operatorEnd

	LPAREN      // (
	RPAREN      // )
	LBRACKET    // {
	RBRACKET    // }
	LSBRACKET   // [
	RSBRACKET   // ]
	COMMA       // ,
	COLON       // :
	DOUBLECOLON // ::
	SEMICOLON   // ;
	DOT         // .

	keywordBeg
	// ALL and the following are Genji SQL Keywords
	AS
	ASC
	BY
	CAST
	CREATE
	DELETE
	DESC
	DROP
	EXISTS
	EXPLAIN
	FROM
	IF
	INDEX
	INSERT
	INTO
	KEY
	LIMIT
	NOT
	OFFSET
	ON
	ORDER
	PRIMARY
	SELECT
	SET
	TABLE
	TO
	UNIQUE
	UNSET
	UPDATE
	VALUES
	WHERE

	TYPEARRAY
	TYPEBLOB
	TYPEBOOL
	TYPEBYTES
	TYPEDOCUMENT
	TYPEDOUBLE
	TYPEDURATION
	TYPEINTEGER
	TYPETEXT

	keywordEnd
)

var tokens = [...]string{
	ILLEGAL: "ILLEGAL",
	EOF:     "EOF",
	WS:      "WS",

	IDENT:           "IDENT",
	POSITIONALPARAM: "?",
	NUMBER:          "NUMBER",
	DURATION:        "DURATIONVAL",
	STRING:          "STRING",
	BADSTRING:       "BADSTRING",
	BADESCAPE:       "BADESCAPE",
	TRUE:            "TRUE",
	FALSE:           "FALSE",
	REGEX:           "REGEX",
	NULL:            "NULL",

	ADD:        "+",
	SUB:        "-",
	MUL:        "*",
	DIV:        "/",
	MOD:        "%",
	BITWISEAND: "&",
	BITWISEOR:  "|",
	BITWISEXOR: "^",

	AND: "AND",
	OR:  "OR",

	EQ:       "=",
	NEQ:      "!=",
	EQREGEX:  "=~",
	NEQREGEX: "!~",
	LT:       "<",
	LTE:      "<=",
	GT:       ">",
	GTE:      ">=",
	IN:       "IN",
	IS:       "IS",

	LPAREN:      "(",
	RPAREN:      ")",
	LBRACKET:    "{",
	RBRACKET:    "}",
	LSBRACKET:   "[",
	RSBRACKET:   "]",
	COMMA:       ",",
	COLON:       ":",
	DOUBLECOLON: "::",
	SEMICOLON:   ";",
	DOT:         ".",

	AS:      "AS",
	ASC:     "ASC",
	BY:      "BY",
	CREATE:  "CREATE",
	CAST:    "CAST",
	DELETE:  "DELETE",
	DESC:    "DESC",
	DROP:    "DROP",
	EXISTS:  "EXISTS",
	EXPLAIN: "EXPLAIN",
	KEY:     "KEY",
	FROM:    "FROM",
	IF:      "IF",
	INDEX:   "INDEX",
	INSERT:  "INSERT",
	INTO:    "INTO",
	LIMIT:   "LIMIT",
	NOT:     "NOT",
	OFFSET:  "OFFSET",
	ON:      "ON",
	ORDER:   "ORDER",
	PRIMARY: "PRIMARY",
	SELECT:  "SELECT",
	SET:     "SET",
	TABLE:   "TABLE",
	TO:      "TO",
	UNIQUE:  "UNIQUE",
	UNSET:   "UNSET",
	UPDATE:  "UPDATE",
	VALUES:  "VALUES",
	WHERE:   "WHERE",

	TYPEARRAY:    "ARRAY",
	TYPEBLOB:     "BLOB",
	TYPEBOOL:     "BOOL",
	TYPEBYTES:    "BYTES",
	TYPEDOCUMENT: "DOCUMENT",
	TYPEDOUBLE:   "DOUBLE",
	TYPEDURATION: "DURATION",
	TYPEINTEGER:  "INTEGER",
	TYPETEXT:     "TEXT",
}

var keywords map[string]Token

func initKeywords() {
	keywords = make(map[string]Token)
	for tok := keywordBeg + 1; tok < keywordEnd; tok++ {
		keywords[strings.ToLower(tokens[tok])] = tok
	}
	for _, tok := range []Token{AND, OR, TRUE, FALSE, NULL, IN, IS} {
		keywords[strings.ToLower(tokens[tok])] = tok
	}
}

// String returns the string representation of the token.
func (tok Token) String() string {
	if tok >= 0 && tok < Token(len(tokens)) {
		return tokens[tok]
	}
	return ""
}

// Precedence returns the operator precedence of the binary operator token.
func (tok Token) Precedence() int {
	switch tok {
	case OR:
		return 1
	case AND:
		return 2
	case IN:
		return 3
	case EQ, NEQ, EQREGEX, NEQREGEX, LT, LTE, GT, GTE, IS:
		return 4
	case ADD, SUB, BITWISEOR, BITWISEXOR:
		return 5
	case MUL, DIV, MOD, BITWISEAND:
		return 6
	}
	return 0
}

// IsOperator returns true for operator tokens.
func (tok Token) IsOperator() bool { return tok > operatorBeg && tok < operatorEnd }

// Tokstr returns a literal if provided, otherwise returns the token string.
func Tokstr(tok Token, lit string) string {
	if lit != "" {
		return lit
	}
	return tok.String()
}

// Lookup returns the token associated with a given string.
func Lookup(ident string) Token {
	if tok, ok := keywords[strings.ToLower(ident)]; ok {
		return tok
	}
	return IDENT
}

// Pos specifies the line and character position of a token.
// The Char and Line are both zero-based indexes.
type Pos struct {
	Line int
	Char int
}
