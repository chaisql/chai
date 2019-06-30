package query

import (
	"errors"
	"fmt"

	"github.com/asdine/genji"
	"github.com/asdine/genji/engine"
	"github.com/asdine/genji/field"
	"github.com/asdine/genji/record"
	"github.com/asdine/genji/table"
)

// Result of a query.
type Result struct {
	t   table.Reader
	err error
}

// Err returns a non nil error if an error occured during the query.
func (q Result) Err() error {
	return q.err
}

// Scan takes a table scanner and passes it the result table.
func (q Result) Scan(s table.Scanner) error {
	if q.err != nil {
		return q.err
	}

	return s.ScanTable(q.t)
}

// Table returns the table result.
func (q Result) Table() table.Reader {
	return q.t
}

// SelectStmt is a DSL that allows creating a full Select query.
// It is typically created using the Select function.
type SelectStmt struct {
	fieldSelectors []FieldSelector
	tableSelector  TableSelector
	whereExpr      Expr
}

// Select creates a DSL equivalent to the SQL Select command.
// It takes a list of field selectors that indicate what fields must be selected from the targeted table.
// This package provides typed field selectors that can be used with the Select method.
func Select(selectors ...FieldSelector) SelectStmt {
	return SelectStmt{fieldSelectors: selectors}
}

// Run the Select query within tx.
// If Where was called, records will be filtered depending on the result of the
// given expression. If the Where expression implements the IndexMatcher interface,
// the MatchIndex method will be called instead of the Eval one.
func (q SelectStmt) Run(tx *genji.Tx) Result {
	if q.tableSelector == nil {
		return Result{err: errors.New("missing table selector")}
	}

	t, err := q.tableSelector.SelectTable(tx)
	if err != nil {
		return Result{err: err}
	}

	var b table.Browser

	if im, ok := q.whereExpr.(IndexMatcher); ok {
		tree, ok, err := im.MatchIndex(tx, q.tableSelector.Name())
		if err != nil && err != engine.ErrIndexNotFound {
			return Result{err: err}
		}

		if ok && err == nil {
			b.Reader = &indexResultTable{
				tree:  tree,
				table: t,
			}
		}
	}

	if b.Reader == nil {
		b.Reader, err = whereClause(tx, t, q.whereExpr)
		if err != nil {
			return Result{err: err}
		}
	}

	b = b.Map(func(rowid []byte, r record.Record) (record.Record, error) {
		var fb record.FieldBuffer

		for _, s := range q.fieldSelectors {
			f, err := s.SelectField(r)
			if err != nil {
				return nil, err
			}

			fb.Add(f)
		}

		return &fb, nil
	})

	if b.Err() != nil {
		return Result{err: b.Err()}
	}

	return Result{t: b.Reader}
}

// Where uses e to filter records if it evaluates to a falsy value.
// Calling this method is optional.
func (q SelectStmt) Where(e Expr) SelectStmt {
	q.whereExpr = e
	return q
}

// From indicates which table to select from.
// Calling this method before Run is mandatory.
func (q SelectStmt) From(tableSelector TableSelector) SelectStmt {
	q.tableSelector = tableSelector
	return q
}

func whereClause(tx *genji.Tx, t table.Reader, e Expr) (table.Reader, error) {
	b := table.NewBrowser(t).Filter(func(_ []byte, r record.Record) (bool, error) {
		sc, err := e.Eval(EvalContext{Tx: tx, Record: r})
		if err != nil {
			return false, err
		}

		return sc.Truthy(), nil
	})
	return b.Reader, b.Err()
}

// DeleteStmt is a DSL that allows creating a full Delete query.
// It is typically created using the Delete function.
type DeleteStmt struct {
	tableSelector TableSelector
	whereExpr     Expr
}

// Delete creates a DSL equivalent to the SQL Delete command.
func Delete() DeleteStmt {
	return DeleteStmt{}
}

// From indicates which table to select from.
// Calling this method before Run is mandatory.
func (d DeleteStmt) From(tableSelector TableSelector) DeleteStmt {
	d.tableSelector = tableSelector
	return d
}

// Where uses e to filter records if it evaluates to a falsy value.
// Calling this method is optional.
func (d DeleteStmt) Where(e Expr) DeleteStmt {
	d.whereExpr = e
	return d
}

// Run the Delete query within tx.
// If Where was called, records will be filtered depending on the result of the
// given expression. If the Where expression implements the IndexMatcher interface,
// the MatchIndex method will be called instead of the Eval one.
func (d DeleteStmt) Run(tx *genji.Tx) error {
	if d.tableSelector == nil {
		return errors.New("missing table selector")
	}

	t, err := d.tableSelector.SelectTable(tx)
	if err != nil {
		return err
	}

	var b table.Browser

	if im, ok := d.whereExpr.(IndexMatcher); ok {
		tree, ok, err := im.MatchIndex(tx, d.tableSelector.Name())
		if err != nil && err != engine.ErrIndexNotFound {
			return err
		}

		if ok && err == nil {
			b.Reader = &indexResultTable{
				tree:  tree,
				table: t,
			}
		}
	}

	if b.Reader == nil {
		b.Reader, err = whereClause(tx, t, d.whereExpr)
		if err != nil {
			return err
		}
	}

	b = b.ForEach(func(rowid []byte, r record.Record) error {
		return t.Delete(rowid)
	})

	return b.Err()
}

// InsertStmt is a DSL that allows creating a full Insert query.
// It is typically created using the Insert function.
type InsertStmt struct {
	tableSelector TableSelector
	fieldNames    []string
	values        []Expr
}

// Insert creates a DSL equivalent to the SQL Insert command.
func Insert() InsertStmt {
	return InsertStmt{}
}

// Into indicates in which table to write the new records.
// Calling this method before Run is mandatory.
func (i InsertStmt) Into(tableSelector TableSelector) InsertStmt {
	i.tableSelector = tableSelector
	return i
}

// Fields to associate with values passed to the Values method.
func (i InsertStmt) Fields(fieldNames ...string) InsertStmt {
	i.fieldNames = append(i.fieldNames, fieldNames...)
	return i
}

// Values to associate with the record fields.
func (i InsertStmt) Values(values ...Expr) InsertStmt {
	i.values = append(i.values, values...)
	return i
}

// Run the Insert query within tx.
// For schemaless tables:
// - If the Fields method was called prior to the Run method, each value will be associated with one of the given field name, in order.
// - If the Fields method wasn't called, this will return an error
//
// For schemafull tables:
// - If the Fields method was called prior to the Run method, each value will be associated with one of the given field name, in order.
// Missing fields will be fields with their zero values.
// - If the Fields method wasn't called, this number of values must match the number of fields of the schema, and each value will be stored in
// each field of the schema, in order.
func (i InsertStmt) Run(tx *genji.Tx) Result {
	if i.tableSelector == nil {
		return Result{err: errors.New("missing table selector")}
	}

	if i.values == nil {
		return Result{err: errors.New("empty values")}
	}

	t, err := i.tableSelector.SelectTable(tx)
	if err != nil {
		return Result{err: err}
	}

	if len(i.fieldNames) == 0 {
		return i.runWithoutSelectedFields(tx, t)
	}

	schema, schemaful := t.Schema()
	if !schemaful {
		return i.runSchemalessWithSelectedFields(tx, t)
	}

	return i.runSchemafulWithSelectedFields(tx, t, &schema)
}

func (i InsertStmt) runWithoutSelectedFields(tx *genji.Tx, t *genji.Table) Result {
	schema, schemaful := t.Schema()

	if !schemaful {
		return Result{err: errors.New("fields must be selected for schemaless tables")}
	}

	var fb record.FieldBuffer

	if len(schema.Fields) != len(i.values) {
		return Result{err: fmt.Errorf("table %s has %d fields, got %d fields", i.tableSelector.Name(), len(schema.Fields), len(i.values))}
	}

	for idx, sf := range schema.Fields {
		sc, err := i.values[idx].Eval(EvalContext{
			Tx: tx,
		})
		if err != nil {
			return Result{err: err}
		}

		if sc.Type != sf.Type {
			return Result{err: fmt.Errorf("cannot assign value of type %q into field of type %q", sc.Type, sf.Type)}
		}

		fb.Add(field.Field{
			Name: sf.Name,
			Type: sf.Type,
			Data: sc.Data,
		})
	}

	rowid, err := t.Insert(&fb)
	if err != nil {
		return Result{err: err}
	}

	return Result{t: rowidToTable(rowid)}
}

func rowidToTable(rowid []byte) table.Table {
	var rb table.RecordBuffer
	rb.Insert(record.FieldBuffer([]field.Field{
		field.NewBytes("rowid", rowid),
	}))
	return &rb
}

func (i InsertStmt) runSchemalessWithSelectedFields(tx *genji.Tx, t *genji.Table) Result {
	var fb record.FieldBuffer

	if len(i.fieldNames) != len(i.values) {
		return Result{err: fmt.Errorf("%d values for %d fields", len(i.values), len(i.fieldNames))}
	}

	for idx, name := range i.fieldNames {
		sc, err := i.values[idx].Eval(EvalContext{
			Tx: tx,
		})
		if err != nil {
			return Result{err: err}
		}

		fb.Add(field.Field{
			Name: name,
			Type: sc.Type,
			Data: sc.Data,
		})
	}

	rowid, err := t.Insert(&fb)
	if err != nil {
		return Result{err: err}
	}

	return Result{t: rowidToTable(rowid)}
}

func (i InsertStmt) runSchemafulWithSelectedFields(tx *genji.Tx, t *genji.Table, schema *record.Schema) Result {
	var fb record.FieldBuffer

	for _, sf := range schema.Fields {
		var found bool
		for idx, name := range i.fieldNames {
			if name != sf.Name {
				continue
			}

			sc, err := i.values[idx].Eval(EvalContext{
				Tx: tx,
			})
			if err != nil {
				return Result{err: err}
			}
			if sc.Type != sf.Type {
				return Result{err: fmt.Errorf("cannot assign value of type %q into field of type %q", sc.Type, sf.Type)}
			}
			fb.Add(field.Field{
				Name: name,
				Type: sc.Type,
				Data: sc.Data,
			})
			found = true
		}

		if !found {
			zv := field.ZeroValue(sf.Type)
			zv.Name = sf.Name
			fb.Add(zv)
		}
	}

	rowid, err := t.Insert(&fb)
	if err != nil {
		return Result{err: err}
	}

	return Result{t: rowidToTable(rowid)}
}
