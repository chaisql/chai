package parser

import (
	"github.com/genjidb/genji/internal/errors"
	"github.com/genjidb/genji/internal/expr"
	"github.com/genjidb/genji/internal/query/statement"
	"github.com/genjidb/genji/internal/sql/scanner"
	"github.com/genjidb/genji/internal/stream"
	"github.com/genjidb/genji/internal/stringutil"
)

// parseSelectStatement parses a select string and returns a Statement AST object.
// This function assumes the SELECT token has already been consumed.
func (p *Parser) parseSelectStatement() (*statement.StreamStmt, error) {
	var stmt statement.SelectStmt
	var err error

	// Parse SELECT ... [UNION | UNION ALL | INTERSECT] SELECT ...
	stmt.CompoundSelect, err = p.parseCompoundSelectStatement()
	if err != nil {
		return nil, err
	}

	// Parse order by: "ORDER BY path [ASC|DESC]?"
	stmt.OrderBy, stmt.OrderByDirection, err = p.parseOrderBy()
	if err != nil {
		return nil, err
	}

	// Parse limit: "LIMIT expr"
	stmt.LimitExpr, err = p.parseLimit()
	if err != nil {
		return nil, err
	}

	// Parse offset: "OFFSET expr"
	stmt.OffsetExpr, err = p.parseOffset()
	if err != nil {
		return nil, err
	}

	return stmt.ToStream()
}

func (p *Parser) parseCompoundSelectStatement() (*statement.StreamStmt, error) {
	var stmt *statement.StreamStmt
	var prev scanner.Token

	var coreStmts []*stream.Stream
	readOnly := true

	for {
		core, err := p.parseSelectCore()
		if err != nil {
			return nil, err
		}
		if !core.ReadOnly {
			readOnly = false
		}

		// Parse optional compound operator
		tok, _, _ := p.ScanIgnoreWhitespace()
		if tok == scanner.UNION {
			all, err := p.parseOptional(scanner.ALL)
			if err != nil {
				return nil, err
			}
			if all {
				tok = scanner.ALL
			}
		}
		if tok != scanner.UNION && tok != scanner.ALL {
			p.Unscan()

			if stmt == nil {
				stmt = core
				break
			}
		}

		coreStmts = append(coreStmts, core.Stream)

		if stmt == nil {
			stmt = core
		}
		if prev != 0 && prev != tok {
			stmt.ReadOnly = readOnly
			switch prev {
			case scanner.UNION:
				stmt.Stream = stream.New(stream.Union(coreStmts...))
			case scanner.ALL:
				stmt.Stream = stream.New(stream.Concat(coreStmts...))
			}

			coreStmts = []*stream.Stream{stmt.Stream}

			if tok != scanner.SELECT && tok != scanner.UNION && tok != scanner.ALL {
				break
			}
		}

		prev = tok
	}

	return stmt, nil
}

func (p *Parser) parseSelectCore() (*statement.StreamStmt, error) {
	var stmt statement.SelectCoreStmt
	var err error

	// Parse "SELECT".
	if err := p.parseTokens(scanner.SELECT); err != nil {
		return nil, err
	}

	stmt.Distinct, err = p.parseDistinct()
	if err != nil {
		return nil, err
	}

	// Parse path list or query.Wildcard
	stmt.ProjectionExprs, err = p.parseProjectedExprs()
	if err != nil {
		return nil, err
	}

	// Parse "FROM".
	stmt.TableName, err = p.parseFrom()
	if err != nil {
		return nil, err
	}

	// Parse condition: "WHERE expr".
	stmt.WhereExpr, err = p.parseCondition()
	if err != nil {
		return nil, err
	}

	// Parse group by: "GROUP BY expr"
	stmt.GroupByExpr, err = p.parseGroupBy()
	if err != nil {
		return nil, err
	}

	return stmt.ToStream()
}

// parseProjectedExprs parses the list of projected fields.
func (p *Parser) parseProjectedExprs() ([]expr.Expr, error) {
	// Parse first (required) result path.
	pe, hasWildcard, err := p.parseProjectedExpr()
	if err != nil {
		return nil, err
	}
	pexprs := []expr.Expr{pe}

	// Parse remaining (optional) result fields.
	for {
		if tok, _, _ := p.ScanIgnoreWhitespace(); tok != scanner.COMMA {
			p.Unscan()
			return pexprs, nil
		}

		pe, wc, err := p.parseProjectedExpr()
		if err != nil {
			return nil, err
		}
		if wc && hasWildcard {
			return nil, stringutil.Errorf("cannot select more than one wildcard")
		}
		hasWildcard = wc

		pexprs = append(pexprs, pe)
	}
}

// parseProjectedExpr parses one projected expression.
func (p *Parser) parseProjectedExpr() (expr.Expr, bool, error) {
	// Check if the * token exists.
	if tok, _, _ := p.ScanIgnoreWhitespace(); tok == scanner.MUL {
		return expr.Wildcard{}, true, nil
	}
	p.Unscan()

	pe, err := p.ParseExpr()
	if err != nil {
		return nil, false, err
	}

	ne := &expr.NamedExpr{Expr: pe}

	// Check if the AS token exists.
	if tok, _, _ := p.ScanIgnoreWhitespace(); tok == scanner.AS {
		ne.ExprName, err = p.parseIdent()
		if err != nil {
			return nil, false, err
		}

		return ne, false, nil
	}
	p.Unscan()

	ne.ExprName = pe.String()

	return ne, false, nil
}

func (p *Parser) parseDistinct() (bool, error) {
	if tok, _, _ := p.ScanIgnoreWhitespace(); tok != scanner.DISTINCT {
		p.Unscan()
		return false, nil
	}

	return true, nil
}

func (p *Parser) parseFrom() (string, error) {
	if ok, err := p.parseOptional(scanner.FROM); !ok || err != nil {
		return "", err
	}

	// Parse table name
	ident, err := p.parseIdent()
	if err != nil {
		pErr := errors.Unwrap(err).(*ParseError)
		pErr.Expected = []string{"table_name"}
		return ident, pErr
	}

	return ident, nil
}

func (p *Parser) parseGroupBy() (expr.Expr, error) {
	ok, err := p.parseOptional(scanner.GROUP, scanner.BY)
	if err != nil || !ok {
		return nil, err
	}

	// parse expr
	e, err := p.ParseExpr()
	return e, err
}

func (p *Parser) parseUnion() (*statement.StreamStmt, bool, error) {
	if ok, err := p.parseOptional(scanner.UNION); !ok || err != nil {
		return nil, false, err
	}

	unionAll, err := p.parseOptional(scanner.ALL)
	if err != nil {
		return nil, false, err
	}

	err = p.parseTokens(scanner.SELECT)
	if err != nil {
		return nil, unionAll, err
	}

	otherSelect, err := p.parseSelectStatement()
	if err != nil {
		return nil, unionAll, err
	}

	return otherSelect, unionAll, nil
}
