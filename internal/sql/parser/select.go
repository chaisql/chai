package parser

import (
	"github.com/cockroachdb/errors"
	"github.com/genjidb/genji/internal/expr"
	"github.com/genjidb/genji/internal/query/statement"
	"github.com/genjidb/genji/internal/sql/scanner"
)

// parseSelectStatement parses a select string and returns a Statement AST object.
// This function assumes the SELECT token has already been consumed.
func (p *Parser) parseSelectStatement() (*statement.SelectStmt, error) {
	stmt := statement.NewSelectStatement()

	// Parse SELECT ... [UNION | UNION ALL | INTERSECT] SELECT ...
	err := p.parseCompoundSelectStatement(stmt)
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

	return stmt, nil
}

func (p *Parser) parseCompoundSelectStatement(stmt *statement.SelectStmt) error {
	for {
		core, err := p.parseSelectCore()
		if err != nil {
			return err
		}

		// Parse optional compound operator
		tok, _, _ := p.ScanIgnoreWhitespace()
		if tok == scanner.UNION {
			all, err := p.parseOptional(scanner.ALL)
			if err != nil {
				return err
			}
			if all {
				tok = scanner.ALL
			}
		}

		stmt.CompoundSelect = append(stmt.CompoundSelect, core)

		if tok != scanner.UNION && tok != scanner.ALL {
			p.Unscan()
			break
		}

		stmt.CompoundOperators = append(stmt.CompoundOperators, tok)
	}

	return nil
}

func (p *Parser) parseSelectCore() (*statement.SelectCoreStmt, error) {
	var stmt statement.SelectCoreStmt
	var err error

	// Parse "SELECT".
	if err := p.parseTokens(scanner.SELECT); err != nil {
		return nil, err
	}

	stmt.Distinct, err = p.parseOptional(scanner.DISTINCT)
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

	return &stmt, nil
}

// parseProjectedExprs parses the list of projected fields.
func (p *Parser) parseProjectedExprs() ([]expr.Expr, error) {
	// Parse first (required) result path.
	pe, err := p.parseProjectedExpr()
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

		pe, err := p.parseProjectedExpr()
		if err != nil {
			return nil, err
		}

		pexprs = append(pexprs, pe)
	}
}

// parseProjectedExpr parses one projected expression.
func (p *Parser) parseProjectedExpr() (expr.Expr, error) {
	// Check if the * token exists.
	if tok, _, _ := p.ScanIgnoreWhitespace(); tok == scanner.MUL {
		return expr.Wildcard{}, nil
	}
	p.Unscan()

	pe, err := p.ParseExpr()
	if err != nil {
		return nil, err
	}

	ne := &expr.NamedExpr{Expr: pe}

	// Check if the AS token exists.
	if tok, _, _ := p.ScanIgnoreWhitespace(); tok == scanner.AS {
		ne.ExprName, err = p.parseIdent()
		if err != nil {
			return nil, err
		}

		return ne, nil
	}
	p.Unscan()

	ne.ExprName = pe.String()

	return ne, nil
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
