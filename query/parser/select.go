package parser

import (
	"github.com/asdine/genji/query"
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
	if tok, pos, lit := p.ScanIgnoreWhitespace(); tok != scanner.FROM {
		return stmt, newParseError(scanner.Tokstr(tok, lit), []string{"FROM"}, pos)
	}

	// Parse table name
	tableName, err := p.ParseIdent()
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
