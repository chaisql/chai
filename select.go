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
	stmt.selectors, err = p.parseResultFields()
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

// parseResultFields parses the list of result fields.
func (p *parser) parseResultFields() ([]resultField, error) {
	// Parse first (required) identifier.
	slctor, err := p.parseResultField()
	if err != nil {
		return nil, err
	}
	selectors := []resultField{slctor}

	// Parse remaining (optional) identifiers.
	for {
		if tok, _, _ := p.ScanIgnoreWhitespace(); tok != scanner.COMMA {
			p.Unscan()
			return selectors, nil
		}

		if slctor, err = p.parseResultField(); err != nil {
			return nil, err
		}

		selectors = append(selectors, slctor)
	}
}

// parseResultField parses the list of selectors.
func (p *parser) parseResultField() (resultField, error) {
	// Check if the * token exists.
	if tok, _, _ := p.ScanIgnoreWhitespace(); tok == scanner.MUL {
		return wildcard{}, nil
	}
	p.Unscan()

	// Check if it's the key() function
	tok, pos, lit := p.ScanIgnoreWhitespace()
	if tok == scanner.KEY {
		if tok, _, _ := p.ScanIgnoreWhitespace(); tok == scanner.LPAREN {
			if tok, pos, lit := p.ScanIgnoreWhitespace(); tok != scanner.RPAREN {
				return nil, newParseError(scanner.Tokstr(tok, lit), []string{")"}, pos)
			}

			return keyFunc{}, nil
		}
	}
	p.Unscan()

	// Check if it's an identifier
	ident, err := p.ParseIdentOrString()
	if err != nil {
		return nil, newParseError(scanner.Tokstr(tok, lit), []string{"ident or string"}, pos)
	}

	return fieldSelector(ident), nil
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
	tableName  string
	whereExpr  expr
	offsetExpr expr
	limitExpr  expr
	selectors  []resultField
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

	opt := newQueryOptimizer(tx, t)
	if err != nil {
		return res, err
	}

	st, err := opt.optimizeQuery(stmt.whereExpr, args)
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

	cfg, err := t.cfgStore.Get(t.name)
	if err != nil {
		return res, err
	}

	st = st.Map(func(r record.Record) (record.Record, error) {
		return recordMask{
			cfg:          cfg,
			r:            r,
			resultFields: stmt.selectors,
		}, nil
	})

	return Result{Stream: st}, nil
}

type recordMask struct {
	cfg          *TableConfig
	r            record.Record
	resultFields []resultField
}

var _ record.Record = recordMask{}

func (r recordMask) GetField(name string) (record.Field, error) {
	for _, rf := range r.resultFields {
		if rf.Name() == name || rf.Name() == "*" {
			return r.r.GetField(name)
		}
	}

	return record.Field{}, fmt.Errorf("field %q not found", name)
}

func (r recordMask) Iterate(fn func(f record.Field) error) error {
	stack := evalStack{
		Record: r.r,
		Cfg:    r.cfg,
	}

	for _, rf := range r.resultFields {
		err := rf.Iterate(stack, fn)
		if err != nil {
			return err
		}
	}

	return nil
}

type resultField interface {
	Iterate(stack evalStack, fn func(fd record.Field) error) error
	Name() string
}

type fieldSelector string

func (f fieldSelector) Name() string {
	return string(f)
}

func (f fieldSelector) SelectField(r record.Record) (record.Field, error) {
	if r == nil {
		return record.Field{}, fmt.Errorf("field not found")
	}

	return r.GetField(string(f))
}

func (f fieldSelector) Iterate(stack evalStack, fn func(fd record.Field) error) error {
	fd, err := f.SelectField(stack.Record)
	if err != nil {
		return nil
	}

	return fn(fd)
}

// Eval extracts the record from the context and selects the right field.
// It implements the Expr interface.
func (f fieldSelector) Eval(stack evalStack) (evalValue, error) {
	fd, err := f.SelectField(stack.Record)
	if err != nil {
		return nilLitteral, nil
	}

	return newSingleEvalValue(fd.Value), nil
}

type wildcard struct{}

func (w wildcard) Name() string {
	return "*"
}

func (w wildcard) Iterate(stack evalStack, fn func(fd record.Field) error) error {
	return stack.Record.Iterate(fn)
}

type keyFunc struct{}

func (k keyFunc) Name() string {
	return "key()"
}

func (k keyFunc) Iterate(stack evalStack, fn func(fd record.Field) error) error {
	if stack.Cfg.PrimaryKeyName != "" {
		fd, err := stack.Record.GetField(stack.Cfg.PrimaryKeyName)
		if err != nil {
			return err
		}
		return fn(fd)
	}

	return fn(record.Field{
		Name: "key()",
		Value: value.Value{
			Data: stack.Record.(record.Keyer).Key(),
			Type: value.Int64,
		},
	})
}
