package testutil

import (
	"database/sql"
	"errors"
	"strings"
	"testing"

	"github.com/chaisql/chai/internal/environment"
	"github.com/chaisql/chai/internal/expr"
	"github.com/chaisql/chai/internal/row"
	"github.com/chaisql/chai/internal/sql/parser"
	"github.com/chaisql/chai/internal/sql/scanner"
	"github.com/stretchr/testify/require"
)

type ResultStream struct {
	*parser.Parser
	env *environment.Environment
}

func (ds *ResultStream) Next() (row.Row, error) {
	return ds.ParseObject()
}

func (p *ResultStream) ParseObject() (row.Row, error) {
	// Parse { token.
	if err := p.Parser.ParseTokens(scanner.LBRACKET); err != nil {
		return nil, err
	}

	var cb row.ColumnBuffer

	// Parse kv pairs.
	for {
		column, e, err := p.parseKV()
		if err != nil {
			p.Unscan()
			break
		}

		v, err := e.Eval(p.env)
		if err != nil {
			return nil, err
		}

		cb.Add(column, v)

		if tok, _, _ := p.ScanIgnoreWhitespace(); tok != scanner.COMMA {
			p.Unscan()
			break
		}
	}

	// Parse required } token.
	if err := p.ParseTokens(scanner.RBRACKET); err != nil {
		return nil, err
	}

	return &cb, nil
}

// parseKV parses a key-value pair in the form IDENT : Expr.
func (p *ResultStream) parseKV() (string, expr.Expr, error) {
	var k string

	tok, _, lit := p.ScanIgnoreWhitespace()
	if tok == scanner.IDENT || tok == scanner.STRING {
		k = lit
	} else {
		return "", nil, errors.New("expected IDENT or STRING")
	}

	if err := p.ParseTokens(scanner.COLON); err != nil {
		p.Unscan()
		return "", nil, err
	}

	e, err := p.ParseExpr()
	if err != nil {
		return "", nil, err
	}

	return k, e, nil
}

func ParseResultStream(stream string) *ResultStream {
	p := parser.NewParser(strings.NewReader(stream))
	env := environment.New(nil, nil, nil, nil)

	return &ResultStream{p, env}
}

func RequireRowsEq(t *testing.T, raw string, rows *sql.Rows) {
	t.Helper()
	RequireRowsEqf(t, raw, rows, "")
}

func RequireRowsEqf(t *testing.T, raw string, rows *sql.Rows, msg string, args ...any) {
	errMsg := append([]any{msg}, args...)
	t.Helper()
	stream := ParseResultStream(raw)

	var want []row.Row

	for {
		v, err := stream.Next()
		if err != nil {
			if perr, ok := err.(*parser.ParseError); ok {
				if perr.Found == "EOF" {
					break
				}
			} else if perr, ok := errors.Unwrap(err).(*parser.ParseError); ok {
				if perr.Found == "EOF" {
					break
				}
			}
		}
		require.NoError(t, err, errMsg...)

		want = append(want, v)
	}

	var got []row.Row

	for rows.Next() {
		cb := SQLRowToColumnBuffer(t, rows)
		got = append(got, cb)
	}
	require.NoError(t, rows.Err(), errMsg...)

	var expected strings.Builder
	for i := range want {
		data, err := row.MarshalTextIndent(want[i], "\n", "  ")
		require.NoError(t, err, errMsg...)
		if i > 0 {
			expected.WriteString("\n")
		}

		expected.WriteString(string(data))
	}

	var actual strings.Builder
	for i := range got {
		data, err := row.MarshalTextIndent(got[i], "\n", "  ")
		require.NoError(t, err, errMsg...)
		if i > 0 {
			actual.WriteString("\n")
		}

		actual.WriteString(string(data))
	}

	if msg != "" {
		require.Equal(t, expected.String(), actual.String(), errMsg...)
	} else {
		require.Equal(t, expected.String(), actual.String())
	}
}
