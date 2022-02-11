package parser

import (
	"github.com/cockroachdb/errors"

	"github.com/genjidb/genji/internal/query/statement"
	"github.com/genjidb/genji/internal/sql/scanner"
)

// parseDeleteStatement parses a delete string and returns a Statement AST object.
func (p *Parser) parseDeleteStatement() (statement.Statement, error) {
	stmt := statement.NewDeleteStatement()
	var err error

	// Parse "DELETE FROM".
	if err := p.parseTokens(scanner.DELETE, scanner.FROM); err != nil {
		return nil, err
	}

	// Parse table name
	stmt.TableName, err = p.parseIdent()
	if err != nil {
		pErr := errors.Unwrap(err).(*ParseError)
		pErr.Expected = []string{"table_name"}
		return nil, pErr
	}

	// Parse condition: "WHERE EXPR".
	stmt.WhereExpr, err = p.parseCondition()
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
