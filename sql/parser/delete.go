package parser

import (
	"github.com/genjidb/genji/expr"
	"github.com/genjidb/genji/query"
	"github.com/genjidb/genji/sql/scanner"
	"github.com/genjidb/genji/stream"
	"github.com/genjidb/genji/stringutil"
)

// parseDeleteStatement parses a delete string and returns a Statement AST object.
// This function assumes the DELETE token has already been consumed.
func (p *Parser) parseDeleteStatement() (*query.StreamStmt, error) {
	var cfg deleteConfig
	var err error

	// Parse "FROM".
	if tok, pos, lit := p.ScanIgnoreWhitespace(); tok != scanner.FROM {
		return nil, newParseError(scanner.Tokstr(tok, lit), []string{"FROM"}, pos)
	}

	// Parse table name
	cfg.TableName, err = p.parseIdent()
	if err != nil {
		pErr := err.(*ParseError)
		pErr.Expected = []string{"table_name"}
		return nil, pErr
	}

	// Parse condition: "WHERE EXPR".
	cfg.WhereExpr, err = p.parseCondition()
	if err != nil {
		return nil, err
	}

	// Parse order by: "ORDER BY path [ASC|DESC]?"
	cfg.OrderBy, cfg.OrderByDirection, err = p.parseOrderBy()
	if err != nil {
		return nil, err
	}

	// Parse limit: "LIMIT expr"
	cfg.LimitExpr, err = p.parseLimit()
	if err != nil {
		return nil, err
	}

	// Parse offset: "OFFSET expr"
	cfg.OffsetExpr, err = p.parseOffset()
	if err != nil {
		return nil, err
	}

	return cfg.ToStream()
}

// DeleteConfig holds DELETE configuration.
type deleteConfig struct {
	TableName        string
	WhereExpr        expr.Expr
	OffsetExpr       expr.Expr
	OrderBy          expr.Path
	LimitExpr        expr.Expr
	OrderByDirection scanner.Token
}

func (cfg deleteConfig) ToStream() (*query.StreamStmt, error) {
	s := stream.New(stream.SeqScan(cfg.TableName))

	if cfg.WhereExpr != nil {
		s = s.Pipe(stream.Filter(cfg.WhereExpr))
	}

	if cfg.OrderBy != nil {
		if cfg.OrderByDirection == scanner.DESC {
			s = s.Pipe(stream.SortReverse(cfg.OrderBy))
		} else {
			s = s.Pipe(stream.Sort(cfg.OrderBy))
		}
	}

	if cfg.OffsetExpr != nil {
		v, err := cfg.OffsetExpr.Eval(&expr.Environment{})
		if err != nil {
			return nil, err
		}

		if !v.Type.IsNumber() {
			return nil, stringutil.Errorf("offset expression must evaluate to a number, got %q", v.Type)
		}

		v, err = v.CastAsInteger()
		if err != nil {
			return nil, err
		}

		s = s.Pipe(stream.Skip(v.V.(int64)))
	}

	if cfg.LimitExpr != nil {
		v, err := cfg.LimitExpr.Eval(&expr.Environment{})
		if err != nil {
			return nil, err
		}

		if !v.Type.IsNumber() {
			return nil, stringutil.Errorf("limit expression must evaluate to a number, got %q", v.Type)
		}

		v, err = v.CastAsInteger()
		if err != nil {
			return nil, err
		}

		s = s.Pipe(stream.Take(v.V.(int64)))
	}

	s = s.Pipe(stream.TableDelete(cfg.TableName))

	return &query.StreamStmt{
		Stream:   s,
		ReadOnly: false,
	}, nil
}
