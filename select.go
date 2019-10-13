package genji

import (
	"database/sql/driver"
	"errors"
	"fmt"

	"github.com/asdine/genji/internal/scanner"
	"github.com/asdine/genji/record"
	"github.com/asdine/genji/value"
)

// parseSelectStatement parses a select string and returns a Statement AST object.
// This function assumes the SELECT token has already been consumed.
func (p *parser) parseSelectStatement() (selectStmt, error) {
	var stmt selectStmt
	var err error

	// Parse field list or wildcard
	stmt.FieldSelectors, err = p.parseFieldNames()
	if err != nil {
		return stmt, err
	}

	// Parse "FROM".
	stmt.tableName, err = p.parseFrom()
	if err != nil {
		return stmt, err
	}

	// Parse condition: "WHERE EXPR".
	stmt.whereExpr, err = p.parseCondition()
	if err != nil {
		return stmt, err
	}

	stmt.limitExpr, err = p.parseLimit()
	if err != nil {
		return stmt, err
	}

	stmt.offsetExpr, err = p.parseOffset()
	if err != nil {
		return stmt, err
	}

	return stmt, nil
}

// parseFieldNames parses the list of field names or a wildward.
func (p *parser) parseFieldNames() ([]fieldSelector, error) {
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
	fselectors := make([]fieldSelector, len(idents))
	for i := range idents {
		fselectors[i] = fieldSelector(idents[i])
	}

	return fselectors, nil
}

func (p *parser) parseFrom() (string, error) {
	if tok, pos, lit := p.ScanIgnoreWhitespace(); tok != scanner.FROM {
		return "", newParseError(scanner.Tokstr(tok, lit), []string{"FROM"}, pos)
	}

	// Parse table name
	return p.ParseIdent()
}

func (p *parser) parseLimit() (expr, error) {
	// parse LIMIT token
	if tok, _, _ := p.ScanIgnoreWhitespace(); tok != scanner.LIMIT {
		p.Unscan()
		return nil, nil
	}

	return p.ParseExpr()
}

func (p *parser) parseOffset() (expr, error) {
	// parse OFFSET token
	if tok, _, _ := p.ScanIgnoreWhitespace(); tok != scanner.OFFSET {
		p.Unscan()
		return nil, nil
	}

	return p.ParseExpr()
}

// selectStmt is a DSL that allows creating a full Select query.
type selectStmt struct {
	tableName      string
	whereExpr      expr
	offsetExpr     expr
	limitExpr      expr
	FieldSelectors []fieldSelector
}

// IsReadOnly always returns true. It implements the Statement interface.
func (stmt selectStmt) IsReadOnly() bool {
	return true
}

// Run the Select statement in the given transaction.
// It implements the Statement interface.
func (stmt selectStmt) Run(tx *Tx, args []driver.NamedValue) (Result, error) {
	return stmt.exec(tx, args)
}

// Exec the Select query within tx.
func (stmt selectStmt) exec(tx *Tx, args []driver.NamedValue) (Result, error) {
	var res Result

	if stmt.tableName == "" {
		return res, errors.New("missing table selector")
	}

	t, err := tx.GetTable(stmt.tableName)
	if err != nil {
		return res, err
	}

	st, err := newQueryOptimizer(tx, t).optimizeQuery(stmt.whereExpr, args)
	if err != nil {
		return res, err
	}

	offset := -1
	limit := -1

	stack := evalStack{
		Tx:     tx,
		Params: args,
	}

	if stmt.offsetExpr != nil {
		v, err := stmt.offsetExpr.Eval(stack)
		if err != nil {
			return res, err
		}

		if v.IsList {
			return res, fmt.Errorf("expected value got list")
		}

		if v.Value.Type < value.Int {
			return res, fmt.Errorf("offset expression must evaluate to a 64 bit integer, got %q", v.Value.Type)
		}

		offset, err = value.DecodeInt(v.Value.Data)
		if err != nil {
			return res, err
		}
	}

	if stmt.limitExpr != nil {
		v, err := stmt.limitExpr.Eval(stack)
		if err != nil {
			return res, err
		}

		if v.IsList {
			return res, fmt.Errorf("expected value got list")
		}

		if v.Value.Type < value.Int {
			return res, fmt.Errorf("limit expression must evaluate to a 64 bit integer, got %q", v.Value.Type)
		}

		limit, err = value.DecodeInt(v.Value.Data)
		if err != nil {
			return res, err
		}
	}

	st = st.Filter(whereClause(stmt.whereExpr, stack))

	if offset > 0 {
		st = st.Offset(offset)
	}

	if limit >= 0 {
		st = st.Limit(limit)
	}

	if len(stmt.FieldSelectors) > 0 {
		fieldNames := make([]string, len(stmt.FieldSelectors))
		for i := range stmt.FieldSelectors {
			fieldNames[i] = stmt.FieldSelectors[i].Name()
		}
		st = st.Map(func(r record.Record) (record.Record, error) {
			return recordMask{
				r:      r,
				fields: fieldNames,
			}, nil
		})
	}

	return Result{Stream: st}, nil
}

type recordMask struct {
	r      record.Record
	fields []string
}

var _ record.Record = recordMask{}

func (r recordMask) GetField(name string) (record.Field, error) {
	for _, n := range r.fields {
		if n == name {
			return r.r.GetField(name)
		}
	}

	return record.Field{}, fmt.Errorf("field %q not found", name)
}

func (r recordMask) Iterate(fn func(f record.Field) error) error {
	for _, n := range r.fields {
		f, err := r.r.GetField(n)
		if err != nil {
			return err
		}

		err = fn(f)
		if err != nil {
			return err
		}
	}

	return nil
}
