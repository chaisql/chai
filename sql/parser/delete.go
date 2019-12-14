package parser

import (
	"github.com/asdine/genji/sql/query"
	"github.com/asdine/genji/sql/scanner"
)

// parseDeleteStatement parses a delete string and returns a Statement AST object.
// This function assumes the DELETE token has already been consumed.
func (p *Parser) parseDeleteStatement() (query.DeleteStmt, error) {
	var stmt query.DeleteStmt
	var err error

	// Parse "FROM".
	if tok, pos, lit := p.ScanIgnoreWhitespace(); tok != scanner.FROM {
		return stmt, newParseError(scanner.Tokstr(tok, lit), []string{"FROM"}, pos)
	}

	// Parse table name
	stmt.TableName, err = p.ParseIdent()
	if err != nil {
		return stmt, err
	}

	// Parse condition: "WHERE EXPR".
	stmt.WhereExpr, err = p.parseCondition()
	if err != nil {
		return stmt, err
	}

	return stmt, nil
}
