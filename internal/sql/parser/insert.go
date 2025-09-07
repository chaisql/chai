package parser

import (
	"fmt"

	"github.com/chaisql/chai/internal/database"
	"github.com/chaisql/chai/internal/expr"
	"github.com/chaisql/chai/internal/query/statement"
	"github.com/chaisql/chai/internal/sql/scanner"
	"github.com/cockroachdb/errors"
)

// parseInsertStatement parses an insert string and returns a Statement AST row.
func (p *Parser) parseInsertStatement() (*statement.InsertStmt, error) {
	var stmt statement.InsertStmt
	var err error

	// Parse "INSERT INTO".
	if err := p.ParseTokens(scanner.INSERT, scanner.INTO); err != nil {
		return nil, err
	}

	// Parse table name
	stmt.TableName, err = p.parseIdent()
	if err != nil {
		pErr := errors.UnwrapAll(err).(*ParseError)
		pErr.Expected = []string{"table_name"}
		return nil, pErr
	}

	// Parse path list: (a, b, c)
	stmt.Columns, err = p.parseSimpleColumnList()
	if err != nil {
		return nil, err
	}

	// Check if VALUES or SELECT token exists.
	tok, pos, lit := p.ScanIgnoreWhitespace()
	switch tok {
	case scanner.VALUES:
		// Parse VALUES (v1, v2, v3)
		stmt.Values, err = p.parseValues(stmt.Columns)
		if err != nil {
			return nil, err
		}
	case scanner.SELECT:
		p.Unscan()
		stmt.SelectStmt, err = p.parseSelectStatement()
		if err != nil {
			return nil, err
		}
	default:
		return nil, newParseError(scanner.Tokstr(tok, lit), []string{"VALUES", "SELECT"}, pos)
	}

	// Parse ON CONFLICT clause
	stmt.OnConflict, err = p.parseOnConflictClause()
	if err != nil {
		return nil, err
	}

	stmt.Returning, err = p.parseReturning()
	if err != nil {
		return nil, err
	}

	return &stmt, nil
}

// parseColumnList parses a list of columns in the form: (column, column, ...), if exists.
// If the list is empty, it returns an error.
func (p *Parser) parseSimpleColumnList() ([]string, error) {
	// Parse ( token.
	if ok, err := p.parseOptional(scanner.LPAREN); !ok || err != nil {
		p.Unscan()
		return nil, err
	}

	// Parse path list.
	var columns []string
	var err error
	if columns, err = p.parseIdentList(); err != nil {
		return nil, err
	}
	set := make(map[string]struct{})
	for _, c := range columns {
		_, ok := set[c]
		if ok {
			return nil, errors.Errorf("column %q specified more than once", c)
		}
		set[c] = struct{}{}
	}

	// Parse required ) token.
	if err := p.ParseTokens(scanner.RPAREN); err != nil {
		return nil, err
	}

	return columns, nil
}

// parseValues parses the "VALUES" clause of the query, if it exists.
func (p *Parser) parseValues(fields []string) ([]expr.Expr, error) {
	var rows []expr.Expr

	// Parse first (required) row.
	r, err := p.parseRowExprList(fields)
	if err != nil {
		return nil, err
	}

	rows = append(rows, r)

	// Parse remaining (optional) rows.
	for {
		if tok, _, _ := p.ScanIgnoreWhitespace(); tok != scanner.COMMA {
			p.Unscan()
			break
		}

		doc, err := p.parseRowExprList(fields)
		if err != nil {
			return nil, err
		}

		rows = append(rows, doc)
	}

	return rows, nil
}

func (p *Parser) parseRowExprList(fields []string) (expr.LiteralExprList, error) {
	list, err := p.parseExprList(scanner.LPAREN, scanner.RPAREN)
	if err != nil {
		return nil, err
	}

	if len(fields) > 0 && len(fields) != len(list) {
		return nil, fmt.Errorf("%d values for %d fields", len(list), len(fields))
	}

	return list, nil
}

func (p *Parser) parseOnConflictClause() (database.OnConflictAction, error) {
	// Parse ON CONFLICT DO clause: ON CONFLICT DO action
	if ok, err := p.parseOptional(scanner.ON, scanner.CONFLICT); !ok || err != nil {
		return 0, err
	}

	tok, pos, lit := p.ScanIgnoreWhitespace()
	// SQLite compatibility: ON CONFLICT [IGNORE | REPLACE]
	switch tok {
	case scanner.IGNORE:
		return database.OnConflictDoNothing, nil
	case scanner.REPLACE:
		return database.OnConflictDoReplace, nil
	}

	// DO [NOTHING | REPLACE]
	if tok != scanner.DO {
		return 0, newParseError(scanner.Tokstr(tok, lit), []string{scanner.DO.String()}, pos)
	}

	tok, pos, lit = p.ScanIgnoreWhitespace()
	switch tok {
	case scanner.NOTHING:
		return database.OnConflictDoNothing, nil
	case scanner.REPLACE:
		return database.OnConflictDoReplace, nil
	}
	return 0, newParseError(scanner.Tokstr(tok, lit), []string{scanner.NOTHING.String(), scanner.REPLACE.String()}, pos)
}

func (p *Parser) parseReturning() ([]expr.Expr, error) {
	// Parse RETURNING clause: RETURNING expr [AS alias]
	if ok, err := p.parseOptional(scanner.RETURNING); !ok || err != nil {
		return nil, err
	}

	return p.parseProjectedExprs()
}
