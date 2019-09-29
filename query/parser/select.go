package parser

import (
	"github.com/asdine/genji/query"
	"github.com/asdine/genji/query/expr"
	"github.com/asdine/genji/query/q"
	"github.com/asdine/genji/query/scanner"
)

// parseSelectStatement parses a select string and returns a query.Statement AST object.
// This function assumes the SELECT token has already been consumed.
func (p *Parser) parseSelectStatement() (query.SelectStmt, error) {
	// Parse field list or wildcard
	fselectors, err := p.parseFieldNames()
	if err != nil {
		return query.Select(), err
	}

	stmt := query.Select(fselectors...)

	// Parse "FROM".
	tableName, err := p.parseFrom()
	if err != nil {
		return stmt, err
	}
	stmt = stmt.From(q.Table(tableName))

	// Parse condition: "WHERE EXPR".
	expr, err := p.parseCondition()
	if err != nil {
		return stmt, err
	}
	stmt = stmt.Where(expr)

	limit, err := p.parseLimit()
	if err != nil {
		return stmt, err
	}
	stmt = stmt.LimitExpr(limit)

	offset, err := p.parseOffset()
	if err != nil {
		return stmt, err
	}
	stmt = stmt.OffsetExpr(offset)

	return stmt, nil
}

// parseFieldNames parses the list of field names or a wildward.
func (p *Parser) parseFieldNames() ([]query.FieldSelector, error) {
	// Check if the * token exists.
	if tok, _, _ := p.ScanIgnoreWhitespace(); tok == scanner.MUL {
		return nil, nil
	}
	p.Unscan()

	// Scan the list of fields
	idents, err := p.ParseIdentList()
	if err != nil {
		return nil, err
	}

	// turn it into field selectors
	fselectors := make([]query.FieldSelector, len(idents))
	for i := range idents {
		fselectors[i] = q.Field(idents[i])
	}

	return fselectors, nil
}

func (p *Parser) parseFrom() (string, error) {
	if tok, pos, lit := p.ScanIgnoreWhitespace(); tok != scanner.FROM {
		return "", newParseError(scanner.Tokstr(tok, lit), []string{"FROM"}, pos)
	}

	// Parse table name
	return p.ParseIdent()
}

func (p *Parser) parseLimit() (expr.Expr, error) {
	// parse LIMIT token
	if tok, _, _ := p.ScanIgnoreWhitespace(); tok != scanner.LIMIT {
		p.Unscan()
		return nil, nil
	}

	return p.ParseExpr()
}

func (p *Parser) parseOffset() (expr.Expr, error) {
	// parse OFFSET token
	if tok, _, _ := p.ScanIgnoreWhitespace(); tok != scanner.OFFSET {
		p.Unscan()
		return nil, nil
	}

	return p.ParseExpr()
}
