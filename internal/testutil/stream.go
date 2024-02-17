package testutil

import (
	"errors"
	"strings"
	"testing"

	"github.com/chaisql/chai"
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
	env := environment.New(nil)

	return &ResultStream{p, env}
}

func RequireStreamEq(t *testing.T, raw string, res *chai.Result) {
	t.Helper()
	RequireStreamEqf(t, raw, res, "")
}

func RequireStreamEqf(t *testing.T, raw string, res *chai.Result, msg string, args ...any) {
	errMsg := append([]any{msg}, args...)
	t.Helper()
	rows := ParseResultStream(raw)

	var want []row.Row

	for {
		v, err := rows.Next()
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

	err := res.Iterate(func(r *chai.Row) error {
		var cb row.ColumnBuffer
		err := r.StructScan(&cb)
		require.NoError(t, err, errMsg...)

		got = append(got, &cb)
		return nil
	})
	require.NoError(t, err, errMsg...)

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
