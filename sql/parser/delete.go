package parser

import (
	"github.com/asdine/genji/query"
	"github.com/asdine/genji/sql/scanner"
)

// parseDeleteStatement parses a delete string and returns a query.Statement AST object.
// This function assumes the DELETE token has already been consumed.
func (p *Parser) parseDeleteStatement() (query.DeleteStmt, error) {
	stmt := query.Delete()

	// Parse "FROM".
	if tok, pos, lit := p.ScanIgnoreWhitespace(); tok != scanner.FROM {
		return stmt, newParseError(scanner.Tokstr(tok, lit), []string{"FROM"}, pos)
	}

	// Parse table name
	tableName, err := p.ParseIdent()
	if err != nil {
		return stmt, err
	}
	stmt = stmt.From(tableName)

	// Parse condition: "WHERE EXPR".
	expr, err := p.parseCondition()
	if err != nil {
		return stmt, err
	}
	stmt = stmt.Where(expr)

	return stmt, nil
}
