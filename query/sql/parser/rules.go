package parser

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/asdine/genji/field"
	"github.com/asdine/genji/query"
)

var g = &grammar{
	rules: []*rule{
		{
			name: "Input",
			pos:  position{line: 5, col: 1, offset: 24},
			expr: &actionExpr{
				pos: position{line: 5, col: 10, offset: 33},
				run: (*parser).callonInput1,
				expr: &seqExpr{
					pos: position{line: 5, col: 10, offset: 33},
					exprs: []interface{}{
						&ruleRefExpr{
							pos:  position{line: 5, col: 10, offset: 33},
							name: "_",
						},
						&labeledExpr{
							pos:   position{line: 5, col: 12, offset: 35},
							label: "stmt",
							expr: &ruleRefExpr{
								pos:  position{line: 5, col: 17, offset: 40},
								name: "Statement",
							},
						},
						&ruleRefExpr{
							pos:  position{line: 5, col: 27, offset: 50},
							name: "_",
						},
						&ruleRefExpr{
							pos:  position{line: 5, col: 29, offset: 52},
							name: "EOF",
						},
					},
				},
			},
		},
		{
			name: "Statement",
			pos:  position{line: 9, col: 1, offset: 82},
			expr: &actionExpr{
				pos: position{line: 9, col: 14, offset: 95},
				run: (*parser).callonStatement1,
				expr: &seqExpr{
					pos: position{line: 9, col: 14, offset: 95},
					exprs: []interface{}{
						&ruleRefExpr{
							pos:  position{line: 9, col: 14, offset: 95},
							name: "_",
						},
						&labeledExpr{
							pos:   position{line: 9, col: 16, offset: 97},
							label: "selectStmt",
							expr: &ruleRefExpr{
								pos:  position{line: 9, col: 27, offset: 108},
								name: "SelectStmt",
							},
						},
					},
				},
			},
		},
		{
			name:        "SelectStmt",
			displayName: "\"Select statement\"",
			pos:         position{line: 13, col: 1, offset: 151},
			expr: &actionExpr{
				pos: position{line: 13, col: 34, offset: 184},
				run: (*parser).callonSelectStmt1,
				expr: &seqExpr{
					pos: position{line: 13, col: 34, offset: 184},
					exprs: []interface{}{
						&ruleRefExpr{
							pos:  position{line: 13, col: 34, offset: 184},
							name: "SelectKeyWord",
						},
						&ruleRefExpr{
							pos:  position{line: 13, col: 48, offset: 198},
							name: "_",
						},
						&labeledExpr{
							pos:   position{line: 13, col: 50, offset: 200},
							label: "from",
							expr: &ruleRefExpr{
								pos:  position{line: 13, col: 55, offset: 205},
								name: "From",
							},
						},
						&ruleRefExpr{
							pos:  position{line: 13, col: 60, offset: 210},
							name: "_",
						},
						&labeledExpr{
							pos:   position{line: 13, col: 62, offset: 212},
							label: "where",
							expr: &zeroOrOneExpr{
								pos: position{line: 13, col: 68, offset: 218},
								expr: &ruleRefExpr{
									pos:  position{line: 13, col: 68, offset: 218},
									name: "Where",
								},
							},
						},
					},
				},
			},
		},
		{
			name: "SelectKeyWord",
			pos:  position{line: 21, col: 1, offset: 365},
			expr: &litMatcher{
				pos:        position{line: 21, col: 18, offset: 382},
				val:        "select",
				ignoreCase: true,
			},
		},
		{
			name: "From",
			pos:  position{line: 23, col: 1, offset: 393},
			expr: &actionExpr{
				pos: position{line: 23, col: 9, offset: 401},
				run: (*parser).callonFrom1,
				expr: &seqExpr{
					pos: position{line: 23, col: 9, offset: 401},
					exprs: []interface{}{
						&ruleRefExpr{
							pos:  position{line: 23, col: 9, offset: 401},
							name: "FromKeyWord",
						},
						&ruleRefExpr{
							pos:  position{line: 23, col: 21, offset: 413},
							name: "_",
						},
						&labeledExpr{
							pos:   position{line: 23, col: 23, offset: 415},
							label: "tableName",
							expr: &ruleRefExpr{
								pos:  position{line: 23, col: 33, offset: 425},
								name: "TableName",
							},
						},
					},
				},
			},
		},
		{
			name: "FromKeyWord",
			pos:  position{line: 27, col: 1, offset: 488},
			expr: &litMatcher{
				pos:        position{line: 27, col: 16, offset: 503},
				val:        "from",
				ignoreCase: true,
			},
		},
		{
			name: "TableName",
			pos:  position{line: 29, col: 1, offset: 512},
			expr: &actionExpr{
				pos: position{line: 29, col: 14, offset: 525},
				run: (*parser).callonTableName1,
				expr: &labeledExpr{
					pos:   position{line: 29, col: 14, offset: 525},
					label: "tableName",
					expr: &ruleRefExpr{
						pos:  position{line: 29, col: 24, offset: 535},
						name: "Word",
					},
				},
			},
		},
		{
			name: "Where",
			pos:  position{line: 33, col: 1, offset: 571},
			expr: &actionExpr{
				pos: position{line: 33, col: 10, offset: 580},
				run: (*parser).callonWhere1,
				expr: &seqExpr{
					pos: position{line: 33, col: 10, offset: 580},
					exprs: []interface{}{
						&ruleRefExpr{
							pos:  position{line: 33, col: 10, offset: 580},
							name: "WhereKeyWord",
						},
						&ruleRefExpr{
							pos:  position{line: 33, col: 23, offset: 593},
							name: "_",
						},
						&labeledExpr{
							pos:   position{line: 33, col: 25, offset: 595},
							label: "expr",
							expr: &ruleRefExpr{
								pos:  position{line: 33, col: 30, offset: 600},
								name: "Expr",
							},
						},
					},
				},
			},
		},
		{
			name: "WhereKeyWord",
			pos:  position{line: 37, col: 1, offset: 631},
			expr: &litMatcher{
				pos:        position{line: 37, col: 17, offset: 647},
				val:        "where",
				ignoreCase: true,
			},
		},
		{
			name: "Expr",
			pos:  position{line: 39, col: 1, offset: 657},
			expr: &choiceExpr{
				pos: position{line: 39, col: 9, offset: 665},
				alternatives: []interface{}{
					&ruleRefExpr{
						pos:  position{line: 39, col: 9, offset: 665},
						name: "Scalar",
					},
					&ruleRefExpr{
						pos:  position{line: 39, col: 18, offset: 674},
						name: "FieldSelector",
					},
				},
			},
		},
		{
			name: "Scalar",
			pos:  position{line: 41, col: 1, offset: 689},
			expr: &choiceExpr{
				pos: position{line: 41, col: 11, offset: 699},
				alternatives: []interface{}{
					&ruleRefExpr{
						pos:  position{line: 41, col: 11, offset: 699},
						name: "StringScalar",
					},
					&ruleRefExpr{
						pos:  position{line: 41, col: 26, offset: 714},
						name: "NumericScalar",
					},
					&ruleRefExpr{
						pos:  position{line: 41, col: 42, offset: 730},
						name: "BoolScalar",
					},
				},
			},
		},
		{
			name: "StringScalar",
			pos:  position{line: 43, col: 1, offset: 742},
			expr: &actionExpr{
				pos: position{line: 43, col: 17, offset: 758},
				run: (*parser).callonStringScalar1,
				expr: &labeledExpr{
					pos:   position{line: 43, col: 17, offset: 758},
					label: "val",
					expr: &choiceExpr{
						pos: position{line: 43, col: 22, offset: 763},
						alternatives: []interface{}{
							&ruleRefExpr{
								pos:  position{line: 43, col: 22, offset: 763},
								name: "SingleQuotedStringScalar",
							},
							&ruleRefExpr{
								pos:  position{line: 43, col: 49, offset: 790},
								name: "DoubleQuotedStringScalar",
							},
						},
					},
				},
			},
		},
		{
			name: "SingleQuotedStringScalar",
			pos:  position{line: 47, col: 1, offset: 899},
			expr: &actionExpr{
				pos: position{line: 47, col: 29, offset: 927},
				run: (*parser).callonSingleQuotedStringScalar1,
				expr: &seqExpr{
					pos: position{line: 47, col: 29, offset: 927},
					exprs: []interface{}{
						&litMatcher{
							pos:        position{line: 47, col: 29, offset: 927},
							val:        "'",
							ignoreCase: false,
						},
						&labeledExpr{
							pos:   position{line: 47, col: 33, offset: 931},
							label: "v",
							expr: &ruleRefExpr{
								pos:  position{line: 47, col: 35, offset: 933},
								name: "Word",
							},
						},
						&litMatcher{
							pos:        position{line: 47, col: 40, offset: 938},
							val:        "'",
							ignoreCase: false,
						},
					},
				},
			},
		},
		{
			name: "DoubleQuotedStringScalar",
			pos:  position{line: 51, col: 1, offset: 965},
			expr: &actionExpr{
				pos: position{line: 51, col: 29, offset: 993},
				run: (*parser).callonDoubleQuotedStringScalar1,
				expr: &seqExpr{
					pos: position{line: 51, col: 29, offset: 993},
					exprs: []interface{}{
						&litMatcher{
							pos:        position{line: 51, col: 29, offset: 993},
							val:        "\"",
							ignoreCase: false,
						},
						&labeledExpr{
							pos:   position{line: 51, col: 33, offset: 997},
							label: "v",
							expr: &ruleRefExpr{
								pos:  position{line: 51, col: 35, offset: 999},
								name: "Word",
							},
						},
						&litMatcher{
							pos:        position{line: 51, col: 40, offset: 1004},
							val:        "\"",
							ignoreCase: false,
						},
					},
				},
			},
		},
		{
			name: "NumericScalar",
			pos:  position{line: 55, col: 1, offset: 1031},
			expr: &actionExpr{
				pos: position{line: 55, col: 17, offset: 1049},
				run: (*parser).callonNumericScalar1,
				expr: &seqExpr{
					pos: position{line: 55, col: 17, offset: 1049},
					exprs: []interface{}{
						&zeroOrOneExpr{
							pos: position{line: 55, col: 17, offset: 1049},
							expr: &litMatcher{
								pos:        position{line: 55, col: 17, offset: 1049},
								val:        "-",
								ignoreCase: false,
							},
						},
						&ruleRefExpr{
							pos:  position{line: 55, col: 22, offset: 1054},
							name: "Integer",
						},
						&labeledExpr{
							pos:   position{line: 55, col: 30, offset: 1062},
							label: "fractional",
							expr: &zeroOrOneExpr{
								pos: position{line: 55, col: 41, offset: 1073},
								expr: &seqExpr{
									pos: position{line: 55, col: 43, offset: 1075},
									exprs: []interface{}{
										&litMatcher{
											pos:        position{line: 55, col: 43, offset: 1075},
											val:        ".",
											ignoreCase: false,
										},
										&oneOrMoreExpr{
											pos: position{line: 55, col: 47, offset: 1079},
											expr: &ruleRefExpr{
												pos:  position{line: 55, col: 47, offset: 1079},
												name: "DecimalDigit",
											},
										},
									},
								},
							},
						},
						&zeroOrOneExpr{
							pos: position{line: 55, col: 64, offset: 1096},
							expr: &ruleRefExpr{
								pos:  position{line: 55, col: 64, offset: 1096},
								name: "Exponent",
							},
						},
					},
				},
			},
		},
		{
			name: "BoolScalar",
			pos:  position{line: 73, col: 1, offset: 1533},
			expr: &actionExpr{
				pos: position{line: 73, col: 15, offset: 1547},
				run: (*parser).callonBoolScalar1,
				expr: &labeledExpr{
					pos:   position{line: 73, col: 15, offset: 1547},
					label: "v",
					expr: &choiceExpr{
						pos: position{line: 73, col: 18, offset: 1550},
						alternatives: []interface{}{
							&litMatcher{
								pos:        position{line: 73, col: 18, offset: 1550},
								val:        "true",
								ignoreCase: false,
							},
							&litMatcher{
								pos:        position{line: 73, col: 27, offset: 1559},
								val:        "TRUE",
								ignoreCase: false,
							},
							&litMatcher{
								pos:        position{line: 73, col: 36, offset: 1568},
								val:        "false",
								ignoreCase: false,
							},
							&litMatcher{
								pos:        position{line: 73, col: 46, offset: 1578},
								val:        "FALSE",
								ignoreCase: false,
							},
						},
					},
				},
			},
		},
		{
			name: "FieldSelector",
			pos:  position{line: 81, col: 1, offset: 1766},
			expr: &actionExpr{
				pos: position{line: 81, col: 18, offset: 1783},
				run: (*parser).callonFieldSelector1,
				expr: &ruleRefExpr{
					pos:  position{line: 81, col: 18, offset: 1783},
					name: "Word",
				},
			},
		},
		{
			name: "Integer",
			pos:  position{line: 85, col: 1, offset: 1829},
			expr: &choiceExpr{
				pos: position{line: 85, col: 11, offset: 1841},
				alternatives: []interface{}{
					&litMatcher{
						pos:        position{line: 85, col: 11, offset: 1841},
						val:        "0",
						ignoreCase: false,
					},
					&seqExpr{
						pos: position{line: 85, col: 17, offset: 1847},
						exprs: []interface{}{
							&ruleRefExpr{
								pos:  position{line: 85, col: 17, offset: 1847},
								name: "NonZeroDecimalDigit",
							},
							&zeroOrMoreExpr{
								pos: position{line: 85, col: 37, offset: 1867},
								expr: &ruleRefExpr{
									pos:  position{line: 85, col: 37, offset: 1867},
									name: "DecimalDigit",
								},
							},
						},
					},
				},
			},
		},
		{
			name: "DecimalDigit",
			pos:  position{line: 87, col: 1, offset: 1882},
			expr: &charClassMatcher{
				pos:        position{line: 87, col: 16, offset: 1899},
				val:        "[0-9]",
				ranges:     []rune{'0', '9'},
				ignoreCase: false,
				inverted:   false,
			},
		},
		{
			name: "NonZeroDecimalDigit",
			pos:  position{line: 89, col: 1, offset: 1906},
			expr: &charClassMatcher{
				pos:        position{line: 89, col: 23, offset: 1930},
				val:        "[1-9]",
				ranges:     []rune{'1', '9'},
				ignoreCase: false,
				inverted:   false,
			},
		},
		{
			name: "Exponent",
			pos:  position{line: 91, col: 1, offset: 1937},
			expr: &seqExpr{
				pos: position{line: 91, col: 12, offset: 1950},
				exprs: []interface{}{
					&litMatcher{
						pos:        position{line: 91, col: 12, offset: 1950},
						val:        "e",
						ignoreCase: true,
					},
					&zeroOrOneExpr{
						pos: position{line: 91, col: 17, offset: 1955},
						expr: &charClassMatcher{
							pos:        position{line: 91, col: 17, offset: 1955},
							val:        "[+-]",
							chars:      []rune{'+', '-'},
							ignoreCase: false,
							inverted:   false,
						},
					},
					&oneOrMoreExpr{
						pos: position{line: 91, col: 23, offset: 1961},
						expr: &ruleRefExpr{
							pos:  position{line: 91, col: 23, offset: 1961},
							name: "DecimalDigit",
						},
					},
				},
			},
		},
		{
			name: "Word",
			pos:  position{line: 93, col: 1, offset: 1976},
			expr: &actionExpr{
				pos: position{line: 93, col: 9, offset: 1984},
				run: (*parser).callonWord1,
				expr: &seqExpr{
					pos: position{line: 93, col: 9, offset: 1984},
					exprs: []interface{}{
						&oneOrMoreExpr{
							pos: position{line: 93, col: 9, offset: 1984},
							expr: &choiceExpr{
								pos: position{line: 93, col: 10, offset: 1985},
								alternatives: []interface{}{
									&ruleRefExpr{
										pos:  position{line: 93, col: 10, offset: 1985},
										name: "EnglishChar",
									},
									&ruleRefExpr{
										pos:  position{line: 93, col: 24, offset: 1999},
										name: "UnicodeChar",
									},
								},
							},
						},
						&zeroOrMoreExpr{
							pos: position{line: 93, col: 38, offset: 2013},
							expr: &choiceExpr{
								pos: position{line: 93, col: 39, offset: 2014},
								alternatives: []interface{}{
									&ruleRefExpr{
										pos:  position{line: 93, col: 39, offset: 2014},
										name: "EnglishChar",
									},
									&ruleRefExpr{
										pos:  position{line: 93, col: 53, offset: 2028},
										name: "UnicodeChar",
									},
									&litMatcher{
										pos:        position{line: 93, col: 67, offset: 2042},
										val:        "_",
										ignoreCase: false,
									},
								},
							},
						},
					},
				},
			},
		},
		{
			name: "EnglishChar",
			pos:  position{line: 97, col: 1, offset: 2084},
			expr: &charClassMatcher{
				pos:        position{line: 97, col: 16, offset: 2099},
				val:        "[a-zA-Z]",
				ranges:     []rune{'a', 'z', 'A', 'Z'},
				ignoreCase: false,
				inverted:   false,
			},
		},
		{
			name: "UnicodeChar",
			pos:  position{line: 99, col: 1, offset: 2109},
			expr: &charClassMatcher{
				pos:        position{line: 99, col: 16, offset: 2124},
				val:        "[^\\u0000-\\u007F]",
				ranges:     []rune{'\x00', '\u007f'},
				ignoreCase: false,
				inverted:   true,
			},
		},
		{
			name:        "_",
			displayName: "\"whitespace\"",
			pos:         position{line: 101, col: 1, offset: 2142},
			expr: &zeroOrMoreExpr{
				pos: position{line: 101, col: 19, offset: 2160},
				expr: &charClassMatcher{
					pos:        position{line: 101, col: 19, offset: 2160},
					val:        "[ \\n\\t\\r]",
					chars:      []rune{' ', '\n', '\t', '\r'},
					ignoreCase: false,
					inverted:   false,
				},
			},
		},
		{
			name: "EOF",
			pos:  position{line: 103, col: 1, offset: 2172},
			expr: &notExpr{
				pos: position{line: 103, col: 8, offset: 2179},
				expr: &anyMatcher{
					line: 103, col: 9, offset: 2180,
				},
			},
		},
	},
}

func (c *current) onInput1(stmt interface{}) (interface{}, error) {
	return stmt, nil
}

func (p *parser) callonInput1() (interface{}, error) {
	stack := p.vstack[len(p.vstack)-1]
	_ = stack
	return p.cur.onInput1(stack["stmt"])
}

func (c *current) onStatement1(selectStmt interface{}) (interface{}, error) {
	return selectStmt, nil
}

func (p *parser) callonStatement1() (interface{}, error) {
	stack := p.vstack[len(p.vstack)-1]
	_ = stack
	return p.cur.onStatement1(stack["selectStmt"])
}

func (c *current) onSelectStmt1(from, where interface{}) (interface{}, error) {
	q := query.Select().From(from.(query.Table))
	if where != nil {
		q = q.Where(where.(query.Expr))
	}
	return q, nil
}

func (p *parser) callonSelectStmt1() (interface{}, error) {
	stack := p.vstack[len(p.vstack)-1]
	_ = stack
	return p.cur.onSelectStmt1(stack["from"], stack["where"])
}

func (c *current) onFrom1(tableName interface{}) (interface{}, error) {
	return query.Table(tableName.(string)), nil
}

func (p *parser) callonFrom1() (interface{}, error) {
	stack := p.vstack[len(p.vstack)-1]
	_ = stack
	return p.cur.onFrom1(stack["tableName"])
}

func (c *current) onTableName1(tableName interface{}) (interface{}, error) {
	return tableName, nil
}

func (p *parser) callonTableName1() (interface{}, error) {
	stack := p.vstack[len(p.vstack)-1]
	_ = stack
	return p.cur.onTableName1(stack["tableName"])
}

func (c *current) onWhere1(expr interface{}) (interface{}, error) {
	return expr, nil
}

func (p *parser) callonWhere1() (interface{}, error) {
	stack := p.vstack[len(p.vstack)-1]
	_ = stack
	return p.cur.onWhere1(stack["expr"])
}

func (c *current) onStringScalar1(val interface{}) (interface{}, error) {
	return &query.Scalar{Type: field.String, Data: []byte(val.(string))}, nil
}

func (p *parser) callonStringScalar1() (interface{}, error) {
	stack := p.vstack[len(p.vstack)-1]
	_ = stack
	return p.cur.onStringScalar1(stack["val"])
}

func (c *current) onSingleQuotedStringScalar1(v interface{}) (interface{}, error) {
	return v, nil
}

func (p *parser) callonSingleQuotedStringScalar1() (interface{}, error) {
	stack := p.vstack[len(p.vstack)-1]
	_ = stack
	return p.cur.onSingleQuotedStringScalar1(stack["v"])
}

func (c *current) onDoubleQuotedStringScalar1(v interface{}) (interface{}, error) {
	return v, nil
}

func (p *parser) callonDoubleQuotedStringScalar1() (interface{}, error) {
	stack := p.vstack[len(p.vstack)-1]
	_ = stack
	return p.cur.onDoubleQuotedStringScalar1(stack["v"])
}

func (c *current) onNumericScalar1(fractional interface{}) (interface{}, error) {
	if fractional != nil {
		f, err := strconv.ParseFloat(string(c.text), 64)
		if err != nil {
			return nil, err
		}

		return &query.Scalar{Type: field.Float64, Data: field.EncodeFloat64(f)}, nil
	}

	i, err := strconv.ParseInt(string(c.text), 10, 64)
	if err != nil {
		return nil, err
	}

	return &query.Scalar{Type: field.Int64, Data: field.EncodeInt64(i)}, nil
}

func (p *parser) callonNumericScalar1() (interface{}, error) {
	stack := p.vstack[len(p.vstack)-1]
	_ = stack
	return p.cur.onNumericScalar1(stack["fractional"])
}

func (c *current) onBoolScalar1(v interface{}) (interface{}, error) {
	x, err := strconv.ParseBool(string(c.text))
	if err != nil {
		return nil, err
	}
	return &query.Scalar{Type: field.Bool, Data: field.EncodeBool(x)}, nil
}

func (p *parser) callonBoolScalar1() (interface{}, error) {
	stack := p.vstack[len(p.vstack)-1]
	_ = stack
	return p.cur.onBoolScalar1(stack["v"])
}

func (c *current) onFieldSelector1() (interface{}, error) {
	return query.Field(c.text), nil
}

func (p *parser) callonFieldSelector1() (interface{}, error) {
	stack := p.vstack[len(p.vstack)-1]
	_ = stack
	return p.cur.onFieldSelector1()
}

func (c *current) onWord1() (interface{}, error) {
	return string(c.text), nil
}

func (p *parser) callonWord1() (interface{}, error) {
	stack := p.vstack[len(p.vstack)-1]
	_ = stack
	return p.cur.onWord1()
}

var (
	// errNoRule is returned when the grammar to parse has no rule.
	errNoRule = errors.New("grammar has no rule")

	// errInvalidEncoding is returned when the source is not properly
	// utf8-encoded.
	errInvalidEncoding = errors.New("invalid encoding")

	// errNoMatch is returned if no match could be found.
	errNoMatch = errors.New("no match found")
)

// Option is a function that can set an option on the parser. It returns
// the previous setting as an Option.
type Option func(*parser) Option

// Debug creates an Option to set the debug flag to b. When set to true,
// debugging information is printed to stdout while parsing.
//
// The default is false.
func Debug(b bool) Option {
	return func(p *parser) Option {
		old := p.debug
		p.debug = b
		return Debug(old)
	}
}

// Memoize creates an Option to set the memoize flag to b. When set to true,
// the parser will cache all results so each expression is evaluated only
// once. This guarantees linear parsing time even for pathological cases,
// at the expense of more memory and slower times for typical cases.
//
// The default is false.
func Memoize(b bool) Option {
	return func(p *parser) Option {
		old := p.memoize
		p.memoize = b
		return Memoize(old)
	}
}

// Recover creates an Option to set the recover flag to b. When set to
// true, this causes the parser to recover from panics and convert it
// to an error. Setting it to false can be useful while debugging to
// access the full stack trace.
//
// The default is true.
func Recover(b bool) Option {
	return func(p *parser) Option {
		old := p.recover
		p.recover = b
		return Recover(old)
	}
}

// ParseFile parses the file identified by filename.
func ParseFile(filename string, opts ...Option) (interface{}, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return ParseReader(filename, f, opts...)
}

// ParseReader parses the data from r using filename as information in the
// error messages.
func ParseReader(filename string, r io.Reader, opts ...Option) (interface{}, error) {
	b, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, err
	}

	return Parse(filename, b, opts...)
}

// Parse parses the data from b using filename as information in the
// error messages.
func Parse(filename string, b []byte, opts ...Option) (interface{}, error) {
	return newParser(filename, b, opts...).parse(g)
}

// position records a position in the text.
type position struct {
	line, col, offset int
}

func (p position) String() string {
	return fmt.Sprintf("%d:%d [%d]", p.line, p.col, p.offset)
}

// savepoint stores all state required to go back to this point in the
// parser.
type savepoint struct {
	position
	rn rune
	w  int
}

type current struct {
	pos  position // start position of the match
	text []byte   // raw text of the match
}

// the AST types...

type grammar struct {
	pos   position
	rules []*rule
}

type rule struct {
	pos         position
	name        string
	displayName string
	expr        interface{}
}

type choiceExpr struct {
	pos          position
	alternatives []interface{}
}

type actionExpr struct {
	pos  position
	expr interface{}
	run  func(*parser) (interface{}, error)
}

type seqExpr struct {
	pos   position
	exprs []interface{}
}

type labeledExpr struct {
	pos   position
	label string
	expr  interface{}
}

type expr struct {
	pos  position
	expr interface{}
}

type andExpr expr
type notExpr expr
type zeroOrOneExpr expr
type zeroOrMoreExpr expr
type oneOrMoreExpr expr

type ruleRefExpr struct {
	pos  position
	name string
}

type andCodeExpr struct {
	pos position
	run func(*parser) (bool, error)
}

type notCodeExpr struct {
	pos position
	run func(*parser) (bool, error)
}

type litMatcher struct {
	pos        position
	val        string
	ignoreCase bool
}

type charClassMatcher struct {
	pos        position
	val        string
	chars      []rune
	ranges     []rune
	classes    []*unicode.RangeTable
	ignoreCase bool
	inverted   bool
}

type anyMatcher position

// errList cumulates the errors found by the parser.
type errList []error

func (e *errList) add(err error) {
	*e = append(*e, err)
}

func (e errList) err() error {
	if len(e) == 0 {
		return nil
	}
	e.dedupe()
	return e
}

func (e *errList) dedupe() {
	var cleaned []error
	set := make(map[string]bool)
	for _, err := range *e {
		if msg := err.Error(); !set[msg] {
			set[msg] = true
			cleaned = append(cleaned, err)
		}
	}
	*e = cleaned
}

func (e errList) Error() string {
	switch len(e) {
	case 0:
		return ""
	case 1:
		return e[0].Error()
	default:
		var buf bytes.Buffer

		for i, err := range e {
			if i > 0 {
				buf.WriteRune('\n')
			}
			buf.WriteString(err.Error())
		}
		return buf.String()
	}
}

// parserError wraps an error with a prefix indicating the rule in which
// the error occurred. The original error is stored in the Inner field.
type parserError struct {
	Inner  error
	pos    position
	prefix string
}

// Error returns the error message.
func (p *parserError) Error() string {
	return p.prefix + ": " + p.Inner.Error()
}

// newParser creates a parser with the specified input source and options.
func newParser(filename string, b []byte, opts ...Option) *parser {
	p := &parser{
		filename: filename,
		errs:     new(errList),
		data:     b,
		pt:       savepoint{position: position{line: 1}},
		recover:  true,
	}
	p.setOptions(opts)
	return p
}

// setOptions applies the options to the parser.
func (p *parser) setOptions(opts []Option) {
	for _, opt := range opts {
		opt(p)
	}
}

type resultTuple struct {
	v   interface{}
	b   bool
	end savepoint
}

type parser struct {
	filename string
	pt       savepoint
	cur      current

	data []byte
	errs *errList

	recover bool
	debug   bool
	depth   int

	memoize bool
	// memoization table for the packrat algorithm:
	// map[offset in source] map[expression or rule] {value, match}
	memo map[int]map[interface{}]resultTuple

	// rules table, maps the rule identifier to the rule node
	rules map[string]*rule
	// variables stack, map of label to value
	vstack []map[string]interface{}
	// rule stack, allows identification of the current rule in errors
	rstack []*rule

	// stats
	exprCnt int
}

// push a variable set on the vstack.
func (p *parser) pushV() {
	if cap(p.vstack) == len(p.vstack) {
		// create new empty slot in the stack
		p.vstack = append(p.vstack, nil)
	} else {
		// slice to 1 more
		p.vstack = p.vstack[:len(p.vstack)+1]
	}

	// get the last args set
	m := p.vstack[len(p.vstack)-1]
	if m != nil && len(m) == 0 {
		// empty map, all good
		return
	}

	m = make(map[string]interface{})
	p.vstack[len(p.vstack)-1] = m
}

// pop a variable set from the vstack.
func (p *parser) popV() {
	// if the map is not empty, clear it
	m := p.vstack[len(p.vstack)-1]
	if len(m) > 0 {
		// GC that map
		p.vstack[len(p.vstack)-1] = nil
	}
	p.vstack = p.vstack[:len(p.vstack)-1]
}

func (p *parser) print(prefix, s string) string {
	if !p.debug {
		return s
	}

	fmt.Printf("%s %d:%d:%d: %s [%#U]\n",
		prefix, p.pt.line, p.pt.col, p.pt.offset, s, p.pt.rn)
	return s
}

func (p *parser) in(s string) string {
	p.depth++
	return p.print(strings.Repeat(" ", p.depth)+">", s)
}

func (p *parser) out(s string) string {
	p.depth--
	return p.print(strings.Repeat(" ", p.depth)+"<", s)
}

func (p *parser) addErr(err error) {
	p.addErrAt(err, p.pt.position)
}

func (p *parser) addErrAt(err error, pos position) {
	var buf bytes.Buffer
	if p.filename != "" {
		buf.WriteString(p.filename)
	}
	if buf.Len() > 0 {
		buf.WriteString(":")
	}
	buf.WriteString(fmt.Sprintf("%d:%d (%d)", pos.line, pos.col, pos.offset))
	if len(p.rstack) > 0 {
		if buf.Len() > 0 {
			buf.WriteString(": ")
		}
		rule := p.rstack[len(p.rstack)-1]
		if rule.displayName != "" {
			buf.WriteString("rule " + rule.displayName)
		} else {
			buf.WriteString("rule " + rule.name)
		}
	}
	pe := &parserError{Inner: err, pos: pos, prefix: buf.String()}
	p.errs.add(pe)
}

// read advances the parser to the next rune.
func (p *parser) read() {
	p.pt.offset += p.pt.w
	rn, n := utf8.DecodeRune(p.data[p.pt.offset:])
	p.pt.rn = rn
	p.pt.w = n
	p.pt.col++
	if rn == '\n' {
		p.pt.line++
		p.pt.col = 0
	}

	if rn == utf8.RuneError {
		if n == 1 {
			p.addErr(errInvalidEncoding)
		}
	}
}

// restore parser position to the savepoint pt.
func (p *parser) restore(pt savepoint) {
	if p.debug {
		defer p.out(p.in("restore"))
	}
	if pt.offset == p.pt.offset {
		return
	}
	p.pt = pt
}

// get the slice of bytes from the savepoint start to the current position.
func (p *parser) sliceFrom(start savepoint) []byte {
	return p.data[start.position.offset:p.pt.position.offset]
}

func (p *parser) getMemoized(node interface{}) (resultTuple, bool) {
	if len(p.memo) == 0 {
		return resultTuple{}, false
	}
	m := p.memo[p.pt.offset]
	if len(m) == 0 {
		return resultTuple{}, false
	}
	res, ok := m[node]
	return res, ok
}

func (p *parser) setMemoized(pt savepoint, node interface{}, tuple resultTuple) {
	if p.memo == nil {
		p.memo = make(map[int]map[interface{}]resultTuple)
	}
	m := p.memo[pt.offset]
	if m == nil {
		m = make(map[interface{}]resultTuple)
		p.memo[pt.offset] = m
	}
	m[node] = tuple
}

func (p *parser) buildRulesTable(g *grammar) {
	p.rules = make(map[string]*rule, len(g.rules))
	for _, r := range g.rules {
		p.rules[r.name] = r
	}
}

func (p *parser) parse(g *grammar) (val interface{}, err error) {
	if len(g.rules) == 0 {
		p.addErr(errNoRule)
		return nil, p.errs.err()
	}

	// TODO : not super critical but this could be generated
	p.buildRulesTable(g)

	if p.recover {
		// panic can be used in action code to stop parsing immediately
		// and return the panic as an error.
		defer func() {
			if e := recover(); e != nil {
				if p.debug {
					defer p.out(p.in("panic handler"))
				}
				val = nil
				switch e := e.(type) {
				case error:
					p.addErr(e)
				default:
					p.addErr(fmt.Errorf("%v", e))
				}
				err = p.errs.err()
			}
		}()
	}

	// start rule is rule [0]
	p.read() // advance to first rune
	val, ok := p.parseRule(g.rules[0])
	if !ok {
		if len(*p.errs) == 0 {
			// make sure this doesn't go out silently
			p.addErr(errNoMatch)
		}
		return nil, p.errs.err()
	}
	return val, p.errs.err()
}

func (p *parser) parseRule(rule *rule) (interface{}, bool) {
	if p.debug {
		defer p.out(p.in("parseRule " + rule.name))
	}

	if p.memoize {
		res, ok := p.getMemoized(rule)
		if ok {
			p.restore(res.end)
			return res.v, res.b
		}
	}

	start := p.pt
	p.rstack = append(p.rstack, rule)
	p.pushV()
	val, ok := p.parseExpr(rule.expr)
	p.popV()
	p.rstack = p.rstack[:len(p.rstack)-1]
	if ok && p.debug {
		p.print(strings.Repeat(" ", p.depth)+"MATCH", string(p.sliceFrom(start)))
	}

	if p.memoize {
		p.setMemoized(start, rule, resultTuple{val, ok, p.pt})
	}
	return val, ok
}

func (p *parser) parseExpr(expr interface{}) (interface{}, bool) {
	var pt savepoint
	var ok bool

	if p.memoize {
		res, ok := p.getMemoized(expr)
		if ok {
			p.restore(res.end)
			return res.v, res.b
		}
		pt = p.pt
	}

	p.exprCnt++
	var val interface{}
	switch expr := expr.(type) {
	case *actionExpr:
		val, ok = p.parseActionExpr(expr)
	case *andCodeExpr:
		val, ok = p.parseAndCodeExpr(expr)
	case *andExpr:
		val, ok = p.parseAndExpr(expr)
	case *anyMatcher:
		val, ok = p.parseAnyMatcher(expr)
	case *charClassMatcher:
		val, ok = p.parseCharClassMatcher(expr)
	case *choiceExpr:
		val, ok = p.parseChoiceExpr(expr)
	case *labeledExpr:
		val, ok = p.parseLabeledExpr(expr)
	case *litMatcher:
		val, ok = p.parseLitMatcher(expr)
	case *notCodeExpr:
		val, ok = p.parseNotCodeExpr(expr)
	case *notExpr:
		val, ok = p.parseNotExpr(expr)
	case *oneOrMoreExpr:
		val, ok = p.parseOneOrMoreExpr(expr)
	case *ruleRefExpr:
		val, ok = p.parseRuleRefExpr(expr)
	case *seqExpr:
		val, ok = p.parseSeqExpr(expr)
	case *zeroOrMoreExpr:
		val, ok = p.parseZeroOrMoreExpr(expr)
	case *zeroOrOneExpr:
		val, ok = p.parseZeroOrOneExpr(expr)
	default:
		panic(fmt.Sprintf("unknown expression type %T", expr))
	}
	if p.memoize {
		p.setMemoized(pt, expr, resultTuple{val, ok, p.pt})
	}
	return val, ok
}

func (p *parser) parseActionExpr(act *actionExpr) (interface{}, bool) {
	if p.debug {
		defer p.out(p.in("parseActionExpr"))
	}

	start := p.pt
	val, ok := p.parseExpr(act.expr)
	if ok {
		p.cur.pos = start.position
		p.cur.text = p.sliceFrom(start)
		actVal, err := act.run(p)
		if err != nil {
			p.addErrAt(err, start.position)
		}
		val = actVal
	}
	if ok && p.debug {
		p.print(strings.Repeat(" ", p.depth)+"MATCH", string(p.sliceFrom(start)))
	}
	return val, ok
}

func (p *parser) parseAndCodeExpr(and *andCodeExpr) (interface{}, bool) {
	if p.debug {
		defer p.out(p.in("parseAndCodeExpr"))
	}

	ok, err := and.run(p)
	if err != nil {
		p.addErr(err)
	}
	return nil, ok
}

func (p *parser) parseAndExpr(and *andExpr) (interface{}, bool) {
	if p.debug {
		defer p.out(p.in("parseAndExpr"))
	}

	pt := p.pt
	p.pushV()
	_, ok := p.parseExpr(and.expr)
	p.popV()
	p.restore(pt)
	return nil, ok
}

func (p *parser) parseAnyMatcher(any *anyMatcher) (interface{}, bool) {
	if p.debug {
		defer p.out(p.in("parseAnyMatcher"))
	}

	if p.pt.rn != utf8.RuneError {
		start := p.pt
		p.read()
		return p.sliceFrom(start), true
	}
	return nil, false
}

func (p *parser) parseCharClassMatcher(chr *charClassMatcher) (interface{}, bool) {
	if p.debug {
		defer p.out(p.in("parseCharClassMatcher"))
	}

	cur := p.pt.rn
	// can't match EOF
	if cur == utf8.RuneError {
		return nil, false
	}
	start := p.pt
	if chr.ignoreCase {
		cur = unicode.ToLower(cur)
	}

	// try to match in the list of available chars
	for _, rn := range chr.chars {
		if rn == cur {
			if chr.inverted {
				return nil, false
			}
			p.read()
			return p.sliceFrom(start), true
		}
	}

	// try to match in the list of ranges
	for i := 0; i < len(chr.ranges); i += 2 {
		if cur >= chr.ranges[i] && cur <= chr.ranges[i+1] {
			if chr.inverted {
				return nil, false
			}
			p.read()
			return p.sliceFrom(start), true
		}
	}

	// try to match in the list of Unicode classes
	for _, cl := range chr.classes {
		if unicode.Is(cl, cur) {
			if chr.inverted {
				return nil, false
			}
			p.read()
			return p.sliceFrom(start), true
		}
	}

	if chr.inverted {
		p.read()
		return p.sliceFrom(start), true
	}
	return nil, false
}

func (p *parser) parseChoiceExpr(ch *choiceExpr) (interface{}, bool) {
	if p.debug {
		defer p.out(p.in("parseChoiceExpr"))
	}

	for _, alt := range ch.alternatives {
		p.pushV()
		val, ok := p.parseExpr(alt)
		p.popV()
		if ok {
			return val, ok
		}
	}
	return nil, false
}

func (p *parser) parseLabeledExpr(lab *labeledExpr) (interface{}, bool) {
	if p.debug {
		defer p.out(p.in("parseLabeledExpr"))
	}

	p.pushV()
	val, ok := p.parseExpr(lab.expr)
	p.popV()
	if ok && lab.label != "" {
		m := p.vstack[len(p.vstack)-1]
		m[lab.label] = val
	}
	return val, ok
}

func (p *parser) parseLitMatcher(lit *litMatcher) (interface{}, bool) {
	if p.debug {
		defer p.out(p.in("parseLitMatcher"))
	}

	start := p.pt
	for _, want := range lit.val {
		cur := p.pt.rn
		if lit.ignoreCase {
			cur = unicode.ToLower(cur)
		}
		if cur != want {
			p.restore(start)
			return nil, false
		}
		p.read()
	}
	return p.sliceFrom(start), true
}

func (p *parser) parseNotCodeExpr(not *notCodeExpr) (interface{}, bool) {
	if p.debug {
		defer p.out(p.in("parseNotCodeExpr"))
	}

	ok, err := not.run(p)
	if err != nil {
		p.addErr(err)
	}
	return nil, !ok
}

func (p *parser) parseNotExpr(not *notExpr) (interface{}, bool) {
	if p.debug {
		defer p.out(p.in("parseNotExpr"))
	}

	pt := p.pt
	p.pushV()
	_, ok := p.parseExpr(not.expr)
	p.popV()
	p.restore(pt)
	return nil, !ok
}

func (p *parser) parseOneOrMoreExpr(expr *oneOrMoreExpr) (interface{}, bool) {
	if p.debug {
		defer p.out(p.in("parseOneOrMoreExpr"))
	}

	var vals []interface{}

	for {
		p.pushV()
		val, ok := p.parseExpr(expr.expr)
		p.popV()
		if !ok {
			if len(vals) == 0 {
				// did not match once, no match
				return nil, false
			}
			return vals, true
		}
		vals = append(vals, val)
	}
}

func (p *parser) parseRuleRefExpr(ref *ruleRefExpr) (interface{}, bool) {
	if p.debug {
		defer p.out(p.in("parseRuleRefExpr " + ref.name))
	}

	if ref.name == "" {
		panic(fmt.Sprintf("%s: invalid rule: missing name", ref.pos))
	}

	rule := p.rules[ref.name]
	if rule == nil {
		p.addErr(fmt.Errorf("undefined rule: %s", ref.name))
		return nil, false
	}
	return p.parseRule(rule)
}

func (p *parser) parseSeqExpr(seq *seqExpr) (interface{}, bool) {
	if p.debug {
		defer p.out(p.in("parseSeqExpr"))
	}

	var vals []interface{}

	pt := p.pt
	for _, expr := range seq.exprs {
		val, ok := p.parseExpr(expr)
		if !ok {
			p.restore(pt)
			return nil, false
		}
		vals = append(vals, val)
	}
	return vals, true
}

func (p *parser) parseZeroOrMoreExpr(expr *zeroOrMoreExpr) (interface{}, bool) {
	if p.debug {
		defer p.out(p.in("parseZeroOrMoreExpr"))
	}

	var vals []interface{}

	for {
		p.pushV()
		val, ok := p.parseExpr(expr.expr)
		p.popV()
		if !ok {
			return vals, true
		}
		vals = append(vals, val)
	}
}

func (p *parser) parseZeroOrOneExpr(expr *zeroOrOneExpr) (interface{}, bool) {
	if p.debug {
		defer p.out(p.in("parseZeroOrOneExpr"))
	}

	p.pushV()
	val, _ := p.parseExpr(expr.expr)
	p.popV()
	// whether it matched or not, consider it a match
	return val, true
}

func rangeTable(class string) *unicode.RangeTable {
	if rt, ok := unicode.Categories[class]; ok {
		return rt
	}
	if rt, ok := unicode.Properties[class]; ok {
		return rt
	}
	if rt, ok := unicode.Scripts[class]; ok {
		return rt
	}

	// cannot happen
	panic(fmt.Sprintf("invalid Unicode class: %s", class))
}
