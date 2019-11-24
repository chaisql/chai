package parser

import (
	"github.com/asdine/genji/internal/scanner"
	"github.com/asdine/genji/query"
)

// parseInsertStatement parses an insert string and returns a Statement AST object.
// This function assumes the INSERT token has already been consumed.
func (p *Parser) parseInsertStatement() (query.InsertStmt, error) {
	var stmt query.InsertStmt
	var err error

	// Parse "INTO".
	if tok, pos, lit := p.ScanIgnoreWhitespace(); tok != scanner.INTO {
		return stmt, newParseError(scanner.Tokstr(tok, lit), []string{"INTO"}, pos)
	}

	// Parse table name
	stmt.TableName, err = p.ParseIdent()
	if err != nil {
		return stmt, err
	}

	// Parse field list: (a, b, c)
	fields, ok, err := p.parseFieldList()
	if err != nil {
		return stmt, err
	}
	if ok {
		stmt.FieldNames = fields
	}

	// Parse VALUES (v1, v2, v3)
	values, found, err := p.parseValues()
	if err != nil {
		return stmt, err
	}
	if found {
		stmt.Values = make(query.LiteralExprList, len(values))
		for i, v := range values {
			stmt.Values[i] = query.LiteralExprList(v)
		}
		return stmt, nil
	}

	// If values was not found, parse RECORDS (r1, r2, r3)
	records, found, err := p.parseRecords()
	if err != nil {
		return stmt, err
	}
	if !found {
		tok, pos, lit := p.ScanIgnoreWhitespace()
		p.Unscan()
		return stmt, newParseError(scanner.Tokstr(tok, lit), []string{"VALUES", "RECORDS"}, pos)
	}

	stmt.Records = records

	return stmt, nil
}

// parseFieldList parses a list of fields in the form: (field, field, ...), if exists
func (p *Parser) parseFieldList() ([]string, bool, error) {
	// Parse ( token.
	if tok, _, _ := p.ScanIgnoreWhitespace(); tok != scanner.LPAREN {
		p.Unscan()
		return nil, false, nil
	}

	// Parse field list.
	var fields []string
	var err error
	if fields, err = p.ParseIdentList(); err != nil {
		return nil, false, err
	}

	// Parse required ) token.
	if tok, pos, lit := p.ScanIgnoreWhitespace(); tok != scanner.RPAREN {
		return nil, false, newParseError(scanner.Tokstr(tok, lit), []string{")"}, pos)
	}

	return fields, true, nil
}

// parseValues parses the "VALUES" clause of the query, if it exists.
func (p *Parser) parseValues() ([]query.LiteralExprList, bool, error) {
	// Check if the VALUES token exists.
	if tok, _, _ := p.ScanIgnoreWhitespace(); tok != scanner.VALUES {
		p.Unscan()
		return nil, false, nil
	}

	var valuesList []query.LiteralExprList
	// Parse first (required) value list.
	exprs, err := p.parseExprList()
	if err != nil {
		return nil, true, err
	}

	valuesList = append(valuesList, query.LiteralExprList(exprs))

	// Parse remaining (optional) values.
	for {
		if tok, _, _ := p.ScanIgnoreWhitespace(); tok != scanner.COMMA {
			p.Unscan()
			break
		}

		values, err := p.parseExprList()
		if err != nil {
			return nil, true, err
		}

		valuesList = append(valuesList, query.LiteralExprList(values))
	}

	return valuesList, true, nil
}

// parseValues parses the "RECORDS" clause of the query, if it exists.
func (p *Parser) parseRecords() ([]interface{}, bool, error) {
	// Check if the RECORDS token exists.
	if tok, _, _ := p.ScanIgnoreWhitespace(); tok != scanner.RECORDS {
		p.Unscan()
		return nil, false, nil
	}

	var records []interface{}

	// Parse first (required) record.
	// It can either be a param or kv list
	rec, err := p.parseRecord()
	if err != nil {
		return nil, false, err
	}

	records = append(records, rec)

	// Parse remaining (optional) records.
	for {
		if tok, _, _ := p.ScanIgnoreWhitespace(); tok != scanner.COMMA {
			p.Unscan()
			break
		}

		rec, err := p.parseRecord()
		if err != nil {
			return nil, false, err
		}

		records = append(records, rec)
	}

	return records, true, nil
}

func (p *Parser) parseRecord() (interface{}, error) {
	// Parse a param first
	v, err := p.parseParam()
	if err != nil {
		p.Unscan()
		return nil, err
	}
	if v != nil {
		return v, nil
	}

	// If not a param, it must be a pairlist
	p.Unscan()

	pairs, ok, err := p.parseKVList()
	if err != nil {
		return nil, err
	}
	if !ok {
		tok, pos, lit := p.ScanIgnoreWhitespace()
		p.Unscan()
		return nil, newParseError(scanner.Tokstr(tok, lit), []string{"record"}, pos)
	}

	return pairs, nil
}

// parseKV parses a key-value pair in the form IDENT : Expr.
func (p *Parser) parseKV() (string, query.Expr, error) {
	k, err := p.ParseIdent()
	if err != nil {
		return "", nil, err
	}

	tok, pos, lit := p.ScanIgnoreWhitespace()
	if tok != scanner.COLON {
		return "", nil, newParseError(scanner.Tokstr(tok, lit), []string{":"}, pos)
	}

	expr, err := p.ParseExpr()
	if err != nil {
		return "", nil, err
	}

	return k, expr, nil
}

// parseKVList parses a list of fields in the form: (k = Expr, k = Expr, ...), if exists
func (p *Parser) parseKVList() ([]query.KVPair, bool, error) {
	// Parse ( token.
	if tok, _, _ := p.ScanIgnoreWhitespace(); tok != scanner.LPAREN {
		p.Unscan()
		return nil, false, nil
	}

	// Parse first (required) identifier.
	k, expr, err := p.parseKV()
	if err != nil {
		return nil, true, err
	}

	pairs := []query.KVPair{query.KVPair{K: k, V: expr}}

	// Parse remaining (optional) identifiers.
	for {
		if tok, _, _ := p.ScanIgnoreWhitespace(); tok != scanner.COMMA {
			p.Unscan()
			break
		}

		if k, expr, err = p.parseKV(); err != nil {
			return nil, true, err
		}

		pairs = append(pairs, query.KVPair{K: k, V: expr})
	}

	// Parse required ) token.
	if tok, pos, lit := p.ScanIgnoreWhitespace(); tok != scanner.RPAREN {
		return nil, true, newParseError(scanner.Tokstr(tok, lit), []string{")"}, pos)
	}

	return pairs, true, nil
}

// parseExprList parses a list of expressions in the form: (expr, expr, ...)
func (p *Parser) parseExprList() ([]query.Expr, error) {
	// Parse ( token.
	if tok, pos, lit := p.ScanIgnoreWhitespace(); tok != scanner.LPAREN {
		return nil, newParseError(scanner.Tokstr(tok, lit), []string{"("}, pos)
	}

	// Parse first (required) expr.
	e, err := p.ParseExpr()
	if err != nil {
		return nil, err
	}
	exprs := []query.Expr{e}

	// Parse remaining (optional) exprs.
	for {
		if tok, _, _ := p.ScanIgnoreWhitespace(); tok != scanner.COMMA {
			p.Unscan()
			break
		}

		if e, err = p.ParseExpr(); err != nil {
			return nil, err
		}

		exprs = append(exprs, e)
	}

	// Parse required ) token.
	if tok, pos, lit := p.ScanIgnoreWhitespace(); tok != scanner.RPAREN {
		return nil, newParseError(scanner.Tokstr(tok, lit), []string{")"}, pos)
	}

	return exprs, nil
}
