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

	// IDENT and the following are Genji SQL literal tokens.
	IDENT           // main
	NAMEDPARAM      // $param
	POSITIONALPARAM // ?
	NUMBER          // 12345.67
	INTEGER         // 12345
	STRING          // "abc"
	BADSTRING       // "abc
	BADESCAPE       // \q
	TRUE            // true
	FALSE           // false
	NULL            // NULL
	REGEX           // Regular expressions
	BADREGEX        // `.*
	ELLIPSIS        // ...
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
	NIN      // NOT IN
	IS       // IS
	ISN      // IS NOT
	LIKE     // LIKE
	NLIKE    // NOT LIKE
	CONCAT   // ||
	BETWEEN  // BETWEEN
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
	ADD_KEYWORD
	ALL
	ALTER
	AS
	ASC
	BEGIN
	BY
	CACHE
	CAST
	CHECK
	COMMIT
	CONFLICT
	CONSTRAINT
	CREATE
	CYCLE
	DEFAULT
	DELETE
	DESC
	DISTINCT
	DO
	DROP
	EXISTS
	EXPLAIN
	FIELD
	FOR
	FROM
	GROUP
	IF
	IGNORE
	INCREMENT
	INDEX
	INSERT
	INTO
	KEY
	LIMIT
	MAXVALUE
	MINVALUE
	NEXT
	NO
	NOT
	NOTHING
	OFFSET
	ON
	ONLY
	ORDER
	PRECISION
	PRIMARY
	READ
	REINDEX
	RENAME
	REPLACE
	RETURNING
	ROLLBACK
	SELECT
	SEQUENCE
	SET
	START
	TABLE
	TO
	TRANSACTION
	UNION
	UNIQUE
	UNSET
	UPDATE
	VALUE
	VALUES
	WITH
	WHERE
	WRITE

	// Types
	TYPEANY
	TYPEARRAY
	TYPEBIGINT
	TYPEBLOB
	TYPEBOOL
	TYPEBOOLEAN
	TYPEBYTES
	TYPECHARACTER
	TYPEDOCUMENT
	TYPEDOUBLE
	TYPEINT
	TYPEINT2
	TYPEINT8
	TYPEINTEGER
	TYPEMEDIUMINT
	TYPESMALLINT
	TYPETEXT
	TYPETINYINT
	TYPEREAL
	TYPEVARCHAR

	keywordEnd
)

var tokens = [...]string{
	ILLEGAL: "ILLEGAL",
	EOF:     "EOF",
	WS:      "WS",

	IDENT:           "IDENT",
	POSITIONALPARAM: "?",
	NUMBER:          "NUMBER",
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
	BETWEEN:    "BETWEEN",

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
	LIKE:     "LIKE",

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

	ADD_KEYWORD: "ADD",
	ALL:         "ALL",
	ALTER:       "ALTER",
	AS:          "AS",
	ASC:         "ASC",
	BEGIN:       "BEGIN",
	BY:          "BY",
	CACHE:       "CACHE",
	CAST:        "CAST",
	CHECK:       "CHECK",
	COMMIT:      "COMMIT",
	CONFLICT:    "CONFLICT",
	CONSTRAINT:  "CONSTRAINT",
	CREATE:      "CREATE",
	CYCLE:       "CYCLE",
	DO:          "DO",
	DEFAULT:     "DEFAULT",
	DELETE:      "DELETE",
	DESC:        "DESC",
	DISTINCT:    "DISTINCT",
	DROP:        "DROP",
	EXISTS:      "EXISTS",
	EXPLAIN:     "EXPLAIN",
	GROUP:       "GROUP",
	KEY:         "KEY",
	FIELD:       "FIELD",
	FOR:         "FOR",
	FROM:        "FROM",
	IF:          "IF",
	IGNORE:      "IGNORE",
	INCREMENT:   "INCREMENT",
	INDEX:       "INDEX",
	INSERT:      "INSERT",
	INTO:        "INTO",
	LIMIT:       "LIMIT",
	MAXVALUE:    "MAXVALUE",
	MINVALUE:    "MINVALUE",
	NEXT:        "NEXT",
	NO:          "NO",
	NOT:         "NOT",
	NOTHING:     "NOTHING",
	OFFSET:      "OFFSET",
	ON:          "ON",
	ONLY:        "ONLY",
	ORDER:       "ORDER",
	PRECISION:   "PRECISION",
	PRIMARY:     "PRIMARY",
	READ:        "READ",
	REINDEX:     "REINDEX",
	RENAME:      "RENAME",
	RETURNING:   "RETURNING",
	REPLACE:     "REPLACE",
	ROLLBACK:    "ROLLBACK",
	START:       "START",
	SELECT:      "SELECT",
	SET:         "SET",
	SEQUENCE:    "SEQUENCE",
	TABLE:       "TABLE",
	TO:          "TO",
	TRANSACTION: "TRANSACTION",
	UNION:       "UNION",
	UNIQUE:      "UNIQUE",
	UNSET:       "UNSET",
	UPDATE:      "UPDATE",
	VALUE:       "VALUE",
	VALUES:      "VALUES",
	WITH:        "WITH",
	WHERE:       "WHERE",
	WRITE:       "WRITE",

	TYPEANY:       "ANY",
	TYPEARRAY:     "ARRAY",
	TYPEBIGINT:    "BIGINT",
	TYPEBLOB:      "BLOB",
	TYPEBOOL:      "BOOL",
	TYPEBOOLEAN:   "BOOLEAN",
	TYPEBYTES:     "BYTES",
	TYPECHARACTER: "CHARACTER",
	TYPEDOCUMENT:  "DOCUMENT",
	TYPEDOUBLE:    "DOUBLE",
	TYPEINT:       "INT",
	TYPEINT2:      "INT2",
	TYPEINT8:      "INT8",
	TYPEINTEGER:   "INTEGER",
	TYPEMEDIUMINT: "MEDIUMINT",
	TYPESMALLINT:  "SMALLINT",
	TYPETEXT:      "TEXT",
	TYPETINYINT:   "TINYINT",
	TYPEREAL:      "REAL",
	TYPEVARCHAR:   "VARCHAR",
}

var keywords map[string]Token

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
	case NOT:
		return 3
	case EQ, NEQ, IS, ISN, IN, NIN, LIKE, NLIKE, EQREGEX, NEQREGEX, BETWEEN:
		return 4
	case LT, LTE, GT, GTE:
		return 5
	case BITWISEOR, BITWISEXOR, BITWISEAND:
		return 6
	case ADD, SUB:
		return 7
	case MUL, DIV, MOD:
		return 8
	case CONCAT:
		return 9
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

// lookup returns the token associated with a given string.
func lookup(ident string) Token {
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

// AllKeywords returns all defined tokens corresponding to keywords.
func AllKeywords() []Token {
	tokens := make([]Token, 0, len(keywords))
	for _, tok := range keywords {
		tokens = append(tokens, tok)
	}
	return tokens
}
