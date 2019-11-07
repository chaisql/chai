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
	IDENTORSTRING   // main
	NAMEDPARAM      // $param
	POSITIONALPARAM // ?
	NUMBER          // 12345.67
	INTEGER         // 12345
	DURATIONVAL     // 13h
	STRING          // "abc"
	BADSTRING       // "abc
	BADESCAPE       // \q
	TRUE            // true
	FALSE           // false
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
	operatorEnd

	LPAREN      // (
	RPAREN      // )
	COMMA       // ,
	COLON       // :
	DOUBLECOLON // ::
	SEMICOLON   // ;
	DOT         // .

	keywordBeg
	// ALL and the following are Genji SQL Keywords
	ALL
	ALTER
	AS
	ASC
	BY
	CREATE
	DELETE
	DESC
	DROP
	DURATION
	EXISTS
	FROM
	IF
	IN
	INDEX
	INF
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
	RECORDS
	TABLE
	TO
	UNIQUE
	UPDATE
	VALUES
	WITH
	WHERE
	keywordEnd

	typesBeg
	TYPEBYTES
	TYPESTRING
	TYPEBOOL
	TYPEINT8
	TYPEINT16
	TYPEINT32
	TYPEINT64
	TYPEINT
	TYPEUINT8
	TYPEUINT16
	TYPEUINT32
	TYPEUINT64
	TYPEUINT
	TYPEFLOAT32
	TYPEFLOAT64
	TYPEINTEGER // alias to TYPEINT
	TYPENUMERIC // alias to TYPEFLOAT64
	TYPETEXT    // alias to TYPESTRING
	typesEnd
)

var tokens = [...]string{
	ILLEGAL: "ILLEGAL",
	EOF:     "EOF",
	WS:      "WS",

	IDENT:           "IDENT",
	IDENTORSTRING:   "IDENTORSTRING",
	POSITIONALPARAM: "?",
	NUMBER:          "NUMBER",
	DURATIONVAL:     "DURATIONVAL",
	STRING:          "STRING",
	BADSTRING:       "BADSTRING",
	BADESCAPE:       "BADESCAPE",
	TRUE:            "TRUE",
	FALSE:           "FALSE",
	REGEX:           "REGEX",

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

	LPAREN:      "(",
	RPAREN:      ")",
	COMMA:       ",",
	COLON:       ":",
	DOUBLECOLON: "::",
	SEMICOLON:   ";",
	DOT:         ".",

	ALL:      "ALL",
	ALTER:    "ALTER",
	AS:       "AS",
	ASC:      "ASC",
	BY:       "BY",
	CREATE:   "CREATE",
	DELETE:   "DELETE",
	DESC:     "DESC",
	DROP:     "DROP",
	DURATION: "DURATION",
	EXISTS:   "EXISTS",
	KEY:      "KEY",
	FROM:     "FROM",
	IF:       "IF",
	IN:       "IN",
	INDEX:    "INDEX",
	INSERT:   "INSERT",
	INTO:     "INTO",
	LIMIT:    "LIMIT",
	NOT:      "NOT",
	OFFSET:   "OFFSET",
	ON:       "ON",
	ORDER:    "ORDER",
	PRIMARY:  "PRIMARY",
	SELECT:   "SELECT",
	SET:      "SET",
	RECORDS:  "RECORDS",
	TABLE:    "TABLE",
	TO:       "TO",
	UNIQUE:   "UNIQUE",
	UPDATE:   "UPDATE",
	VALUES:   "VALUES",
	WITH:     "WITH",
	WHERE:    "WHERE",

	TYPEBYTES:   "BYTES",
	TYPESTRING:  "STRING",
	TYPEBOOL:    "BOOL",
	TYPEINT8:    "INT8",
	TYPEINT16:   "INT16",
	TYPEINT32:   "INT32",
	TYPEINT64:   "INT64",
	TYPEINT:     "INT",
	TYPEUINT8:   "UINT8",
	TYPEUINT16:  "UINT16",
	TYPEUINT32:  "UINT32",
	TYPEUINT64:  "UINT64",
	TYPEUINT:    "UINT",
	TYPEFLOAT32: "FLOAT32",
	TYPEFLOAT64: "FLOAT64",
	TYPEINTEGER: "INTEGER",
	TYPENUMERIC: "NUMERIC",
	TYPETEXT:    "TEXT",
}

var keywords map[string]Token

func init() {
	keywords = make(map[string]Token)
	for tok := keywordBeg + 1; tok < keywordEnd; tok++ {
		keywords[strings.ToLower(tokens[tok])] = tok
	}
	for _, tok := range []Token{AND, OR} {
		keywords[strings.ToLower(tokens[tok])] = tok
	}
	keywords["true"] = TRUE
	keywords["false"] = FALSE
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
	case EQ, NEQ, EQREGEX, NEQREGEX, LT, LTE, GT, GTE:
		return 3
	case ADD, SUB, BITWISEOR, BITWISEXOR:
		return 4
	case MUL, DIV, MOD, BITWISEAND:
		return 5
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
