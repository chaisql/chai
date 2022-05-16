package parser

import (
	"fmt"

	"github.com/cockroachdb/errors"
	"github.com/genjidb/genji/internal/database"
	"github.com/genjidb/genji/internal/expr"
	"github.com/genjidb/genji/internal/query/statement"
	"github.com/genjidb/genji/internal/sql/scanner"
)

// parseInsertStatement parses an insert string and returns a Statement AST object.
func (p *Parser) parseInsertStatement() (*statement.InsertStmt, error) {
	stmt := statement.NewInsertStatement()
	var err error

	// Parse "INSERT INTO".
	if err := p.parseTokens(scanner.INSERT, scanner.INTO); err != nil {
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
	stmt.Fields, err = p.parseFieldList()
	if err != nil {
		return nil, err
	}

	// Check if VALUES or SELECT token exists.
	tok, pos, lit := p.ScanIgnoreWhitespace()
	switch tok {
	case scanner.VALUES:
		// Parse VALUES (v1, v2, v3)
		stmt.Values, err = p.parseValues(stmt.Fields)
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

	return stmt, nil
}

// parseFieldList parses a list of fields in the form: (path, path, ...), if exists.
// If the list is empty, it returns an error.
func (p *Parser) parseFieldList() ([]string, error) {
	// Parse ( token.
	if ok, err := p.parseOptional(scanner.LPAREN); !ok || err != nil {
		p.Unscan()
		return nil, err
	}

	// Parse path list.
	var fields []string
	var err error
	if fields, err = p.parseIdentList(); err != nil {
		return nil, err
	}

	// Parse required ) token.
	if err := p.parseTokens(scanner.RPAREN); err != nil {
		return nil, err
	}

	return fields, nil
}

// parseValues parses the "VALUES" clause of the query, if it exists.
func (p *Parser) parseValues(fields []string) ([]expr.Expr, error) {
	if len(fields) > 0 {
		return p.parseDocumentsWithFields(fields)
	}

	tok, pos, lit := p.ScanIgnoreWhitespace()
	p.Unscan()
	switch tok {
	case scanner.LPAREN:
		return p.parseDocumentsWithFields(fields)
	case scanner.LBRACKET, scanner.NAMEDPARAM, scanner.POSITIONALPARAM:
		return p.parseLiteralDocOrParamList()
	}

	return nil, newParseError(scanner.Tokstr(tok, lit), []string{"(", "[", "?", "$"}, pos)
}

// parseExprListValues parses the "VALUES" clause of the query, if it exists.
func (p *Parser) parseDocumentsWithFields(fields []string) ([]expr.Expr, error) {
	var docs []expr.Expr

	// Parse first (required) value list.
	doc, err := p.parseExprListWithFields(fields)
	if err != nil {
		return nil, err
	}

	docs = append(docs, doc)

	// Parse remaining (optional) values.
	for {
		if tok, _, _ := p.ScanIgnoreWhitespace(); tok != scanner.COMMA {
			p.Unscan()
			break
		}

		doc, err := p.parseExprListWithFields(fields)
		if err != nil {
			return nil, err
		}

		docs = append(docs, doc)
	}

	return docs, nil
}

// parseParamOrDocument parses either a parameter or a document.
func (p *Parser) parseExprListWithFields(fields []string) (*expr.KVPairs, error) {
	list, err := p.parseExprList(scanner.LPAREN, scanner.RPAREN)
	if err != nil {
		return nil, err
	}

	var pairs expr.KVPairs
	pairs.Pairs = make([]expr.KVPair, len(list))

	if len(fields) > 0 {
		if len(fields) != len(list) {
			return nil, fmt.Errorf("%d values for %d fields", len(list), len(fields))
		}

		for i := range list {
			pairs.Pairs[i].K = fields[i]
			pairs.Pairs[i].V = list[i]
		}
	} else {
		for i := range list {
			pairs.Pairs[i].V = list[i]
		}
	}

	return &pairs, nil
}

// parseExprListValues parses the "VALUES" clause of the query, if it exists.
func (p *Parser) parseLiteralDocOrParamList() ([]expr.Expr, error) {
	var docs []expr.Expr

	// Parse first (required) value list.
	doc, err := p.parseParamOrDocument()
	if err != nil {
		return nil, err
	}

	docs = append(docs, doc)

	// Parse remaining (optional) values.
	for {
		if tok, _, _ := p.ScanIgnoreWhitespace(); tok != scanner.COMMA {
			p.Unscan()
			break
		}

		doc, err := p.parseParamOrDocument()
		if err != nil {
			return nil, err
		}

		docs = append(docs, doc)
	}

	return docs, nil
}

// parseParamOrDocument parses either a parameter or a document.
func (p *Parser) parseParamOrDocument() (expr.Expr, error) {
	// Parse a param first
	prm, err := p.parseParam()
	if err != nil {
		return nil, err
	}
	if prm != nil {
		return prm, nil
	}

	// If not a param, start over
	p.Unscan()

	// Expect a document
	return p.ParseDocument()
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
