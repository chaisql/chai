package parser

import (
	"fmt"

	"github.com/genjidb/genji/planner"
	"github.com/genjidb/genji/query/expr"
	"github.com/genjidb/genji/sql/scanner"
	"github.com/genjidb/genji/stream"
)

// parseInsertStatement parses an insert string and returns a Statement AST object.
// This function assumes the INSERT token has already been consumed.
func (p *Parser) parseInsertStatement() (*planner.Statement, error) {
	var cfg insertConfig
	var err error

	// Parse "INTO".
	if tok, pos, lit := p.ScanIgnoreWhitespace(); tok != scanner.INTO {
		return nil, newParseError(scanner.Tokstr(tok, lit), []string{"INTO"}, pos)
	}

	// Parse table name
	cfg.TableName, err = p.parseIdent()
	if err != nil {
		pErr := err.(*ParseError)
		pErr.Expected = []string{"table_name"}
		return nil, pErr
	}

	// Parse path list: (a, b, c)
	fields, err := p.parseFieldList()
	if err != nil {
		return nil, err
	}

	// Parse VALUES (v1, v2, v3)
	cfg.Values, err = p.parseValues(fields)
	if err != nil {
		return nil, err
	}

	return cfg.ToStream(), nil
}

// parseFieldList parses a list of fields in the form: (path, path, ...), if exists.
// If the list is empty, it returns an error.
func (p *Parser) parseFieldList() ([]string, error) {
	// Parse ( token.
	if tok, _, _ := p.ScanIgnoreWhitespace(); tok != scanner.LPAREN {
		p.Unscan()
		return nil, nil
	}

	// Parse path list.
	var fields []string
	var err error
	if fields, err = p.parseIdentList(); err != nil {
		return nil, err
	}

	// Parse required ) token.
	if tok, pos, lit := p.ScanIgnoreWhitespace(); tok != scanner.RPAREN {
		return nil, newParseError(scanner.Tokstr(tok, lit), []string{")"}, pos)
	}

	return fields, nil
}

// parseValues parses the "VALUES" clause of the query, if it exists.
func (p *Parser) parseValues(fields []string) ([]expr.Expr, error) {
	// Check if the VALUES token exists.
	if tok, pos, lit := p.ScanIgnoreWhitespace(); tok != scanner.VALUES {
		return nil, newParseError(scanner.Tokstr(tok, lit), []string{"VALUES"}, pos)
	}

	if len(fields) > 0 {
		return p.parseDocumentsWithFields(fields)
	}

	return p.parseLiteralDocOrParamList()
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

	if len(fields) != len(list) {
		return nil, fmt.Errorf("%d values for %d fields", len(list), len(fields))
	}

	for i := range list {
		pairs.Pairs[i].K = fields[i]
		pairs.Pairs[i].V = list[i]
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
	return p.parseDocument()
}

// insertConfig holds INSERT configuration.
type insertConfig struct {
	TableName string
	Values    []expr.Expr
}

func (cfg *insertConfig) ToStream() *planner.Statement {
	s := stream.New(stream.Expressions(cfg.Values...))

	s = s.Pipe(stream.TableInsert(cfg.TableName))

	return &planner.Statement{
		Stream:   s,
		ReadOnly: false,
	}
}
