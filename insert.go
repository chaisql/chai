package genji

import (
	"database/sql/driver"
	"errors"
	"fmt"

	"github.com/asdine/genji/database"
	"github.com/asdine/genji/query/expr"
	"github.com/asdine/genji/record"
	"github.com/asdine/genji/sql/scanner"
	"github.com/asdine/genji/value"
)

// parseInsertStatement parses an insert string and returns a Statement AST object.
// This function assumes the INSERT token has already been consumed.
func (p *Parser) parseInsertStatement() (insertStmt, error) {
	var stmt insertStmt
	var err error

	// Parse "INTO".
	if tok, pos, lit := p.ScanIgnoreWhitespace(); tok != scanner.INTO {
		return stmt, newParseError(scanner.Tokstr(tok, lit), []string{"INTO"}, pos)
	}

	// Parse table name
	stmt.tableName, err = p.ParseIdent()
	if err != nil {
		return stmt, err
	}

	// Parse field list: (a, b, c)
	fields, ok, err := p.parseFieldList()
	if err != nil {
		return stmt, err
	}
	if ok {
		stmt.fieldNames = fields
	}

	// Parse VALUES (v1, v2, v3)
	values, found, err := p.parseValues()
	if err != nil {
		return stmt, err
	}
	if found {
		stmt.values = make(expr.LitteralExprList, len(values))
		for i, v := range values {
			stmt.values[i] = expr.LitteralExprList(v)
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

	stmt.records = records

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
func (p *Parser) parseValues() ([]expr.LitteralExprList, bool, error) {
	// Check if the VALUES token exists.
	if tok, _, _ := p.ScanIgnoreWhitespace(); tok != scanner.VALUES {
		p.Unscan()
		return nil, false, nil
	}

	var valuesList []expr.LitteralExprList
	// Parse first (required) value list.
	exprs, err := p.parseExprList()
	if err != nil {
		return nil, true, err
	}

	valuesList = append(valuesList, expr.LitteralExprList(exprs))

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

		valuesList = append(valuesList, expr.LitteralExprList(values))
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

// parseKV parses a key-value pair in the form IDENT : expr.Expr.
func (p *Parser) parseKV() (string, expr.Expr, error) {
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
func (p *Parser) parseKVList() ([]kvPair, bool, error) {
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

	pairs := []kvPair{kvPair{K: k, V: expr}}

	// Parse remaining (optional) identifiers.
	for {
		if tok, _, _ := p.ScanIgnoreWhitespace(); tok != scanner.COMMA {
			p.Unscan()
			break
		}

		if k, expr, err = p.parseKV(); err != nil {
			return nil, true, err
		}

		pairs = append(pairs, kvPair{K: k, V: expr})
	}

	// Parse required ) token.
	if tok, pos, lit := p.ScanIgnoreWhitespace(); tok != scanner.RPAREN {
		return nil, true, newParseError(scanner.Tokstr(tok, lit), []string{")"}, pos)
	}

	return pairs, true, nil
}

// parseExprList parses a list of expressions in the form: (expr, expr, ...)
func (p *Parser) parseExprList() ([]expr.Expr, error) {
	// Parse ( token.
	if tok, pos, lit := p.ScanIgnoreWhitespace(); tok != scanner.LPAREN {
		return nil, newParseError(scanner.Tokstr(tok, lit), []string{"("}, pos)
	}

	// Parse first (required) expr.
	e, err := p.ParseExpr()
	if err != nil {
		return nil, err
	}
	exprs := []expr.Expr{e}

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

// insertStmt is a DSL that allows creating a full Insert query.
type insertStmt struct {
	tableName  string
	fieldNames []string
	values     expr.LitteralExprList
	records    []interface{}
}

// IsReadOnly always returns false. It implements the Statement interface.
func (stmt insertStmt) IsReadOnly() bool {
	return false
}

type kvPair struct {
	K string
	V expr.Expr
}

func (stmt insertStmt) Pairs(pairs ...kvPair) insertStmt {
	stmt.records = append(stmt.records, pairs)

	return stmt
}

func (stmt insertStmt) Run(tx *database.Tx, args []driver.NamedValue) Result {
	if stmt.tableName == "" {
		return Result{err: errors.New("missing table name")}
	}

	if stmt.values == nil && stmt.records == nil {
		return Result{err: errors.New("values and records are empty")}
	}

	t, err := tx.GetTable(stmt.tableName)
	if err != nil {
		return Result{err: err}
	}

	stack := expr.EvalStack{
		Tx:     tx,
		Params: args,
	}

	if len(stmt.records) > 0 {
		return stmt.insertRecords(t, stack)
	}

	return stmt.insertValues(t, stack)
}

type paramExtractor interface {
	Extract(params []driver.NamedValue) (interface{}, error)
}

func (stmt insertStmt) insertRecords(t *database.Table, stack expr.EvalStack) Result {
	if len(stmt.fieldNames) > 0 {
		return Result{err: errors.New("can't provide a field list with RECORDS clause")}
	}

	var res Result
	var err error

	for _, rec := range stmt.records {
		var r record.Record

		switch tp := rec.(type) {
		case record.Record:
			r = tp
		case paramExtractor:
			v, err := tp.Extract(stack.Params)
			if err != nil {
				return Result{err: err}
			}

			var ok bool
			r, ok = v.(record.Record)
			if !ok {
				return Result{err: fmt.Errorf("unsupported parameter of type %t, expecting record.Record", v)}
			}
		case []kvPair:
			var fb record.FieldBuffer
			for _, pair := range tp {
				v, err := pair.V.Eval(stack)
				if err != nil {
					res.err = err
					return res
				}

				if v.IsList {
					res.err = errors.New("invalid values")
					return res
				}

				fb.Add(record.Field{Name: pair.K, Value: v.Value.Value})
			}
			r = &fb
		}

		res.lastInsertRecordID, err = t.Insert(r)
		if err != nil {
			return Result{err: err}
		}

		res.rowsAffected++
	}

	res.Stream = record.NewStream(record.NewIteratorFromRecords())
	return res
}

func (stmt insertStmt) insertValues(t *database.Table, stack expr.EvalStack) Result {
	var res Result

	// iterate over all of the records (r1, r2, r3, ...)
	for _, e := range stmt.values {
		var fb record.FieldBuffer

		v, err := e.Eval(stack)
		if err != nil {
			return Result{err: err}
		}

		// each record must be a list of values
		// (e1, e2, e3, ...)
		if !v.IsList {
			return Result{err: errors.New("invalid values")}
		}

		if len(stmt.fieldNames) != len(v.List) {
			return Result{err: fmt.Errorf("%d values for %d fields", len(v.List), len(stmt.fieldNames))}
		}

		// iterate over each value
		for i, v := range v.List {
			// get the field name
			fieldName := stmt.fieldNames[i]

			var lv *expr.LitteralValue

			// each value must be either a LitteralValue or a LitteralValueList with exactly
			// one value
			if !v.IsList {
				lv = &v.Value
			} else {
				if len(v.List) == 1 {
					if val := v.List[0]; !val.IsList {
						lv = &val.Value
					}
				}
				return Result{err: fmt.Errorf("value expected, got list")}
			}

			// Assign the value to the field and add it to the record
			fb.Add(record.Field{
				Name: fieldName,
				Value: value.Value{
					Type: lv.Type,
					Data: lv.Data,
				},
			})
		}

		res.lastInsertRecordID, err = t.Insert(&fb)
		if err != nil {
			return Result{err: err}
		}

		res.rowsAffected++
	}

	res.Stream = record.NewStream(record.NewIteratorFromRecords())

	return res
}
