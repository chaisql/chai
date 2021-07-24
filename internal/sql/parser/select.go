package parser

import (
	"github.com/genjidb/genji/internal/expr"
	"github.com/genjidb/genji/internal/query/statement"
	"github.com/genjidb/genji/internal/sql/scanner"
)

// parseSelectStatement parses a select string and returns a Statement AST object.
// This function assumes the SELECT token has already been consumed.
func (p *Parser) parseSelectStatement() (*statement.StreamStmt, error) {
	var stmt statement.SelectStmt
	var err error

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
	var found bool
	stmt.TableName, found, err = p.parseFrom()
	if err != nil {
		return nil, err
	}
	if !found {
		return stmt.ToStream()
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

	// Parse union: "UNION expr"
	stmt.Union.SelectStmt, stmt.Union.All, err = p.parseUnion()
	if err != nil {
		return nil, err
	}

	return stmt.ToStream()
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

		if pe, err = p.parseProjectedExpr(); err != nil {
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

func (p *Parser) parseDistinct() (bool, error) {
	if tok, _, _ := p.ScanIgnoreWhitespace(); tok != scanner.DISTINCT {
		p.Unscan()
		return false, nil
	}

	return true, nil
}

func (p *Parser) parseFrom() (string, bool, error) {
	if ok, err := p.parseOptional(scanner.FROM); !ok || err != nil {
		return "", false, err
	}

	// Parse table name
	ident, err := p.parseIdent()
	if err != nil {
		pErr := err.(*ParseError)
		pErr.Expected = []string{"table_name"}
		return ident, true, pErr
	}

	return ident, true, nil
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
	// Only UNION ALL is supported for the moment
	if ok, err := p.parseOptional(scanner.UNION, scanner.ALL); !ok || err != nil {
		return nil, false, err
	}

	err := p.parseTokens(scanner.SELECT)
	if err != nil {
		return nil, false, err
	}

	otherSelect, err := p.parseSelectStatement()
	if err != nil {
		return nil, false, err
	}

	return otherSelect, false, nil
}
