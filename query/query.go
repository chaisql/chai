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

type Result struct {
	t   table.Reader
	err error
}

func (q Result) Err() error {
	return q.err
}

func (q Result) Scan(s table.Scanner) error {
	if q.err != nil {
		return q.err
	}

	return s.ScanTable(q.t)
}

func (q Result) Table() table.Reader {
	return q.t
}

type SelectStmt struct {
	fieldSelectors []FieldSelector
	tableSelector  TableSelector
	whereExpr      Expr
}

func Select(selectors ...FieldSelector) SelectStmt {
	return SelectStmt{fieldSelectors: selectors}
}

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

func (q SelectStmt) Where(e Expr) SelectStmt {
	q.whereExpr = e
	return q
}

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

type DeleteStmt struct {
	tableSelector TableSelector
	whereExpr     Expr
}

func Delete() DeleteStmt {
	return DeleteStmt{}
}

func (s DeleteStmt) From(tableSelector TableSelector) DeleteStmt {
	s.tableSelector = tableSelector
	return s
}

func (s DeleteStmt) Where(e Expr) DeleteStmt {
	s.whereExpr = e
	return s
}

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

type InsertStmt struct {
	tableSelector TableSelector
	fieldNames    []string
	values        []Expr
}

func Insert() InsertStmt {
	return InsertStmt{}
}

func (i InsertStmt) Into(tableSelector TableSelector) InsertStmt {
	i.tableSelector = tableSelector
	return i
}

func (i InsertStmt) Fields(fieldNames ...string) InsertStmt {
	i.fieldNames = append(i.fieldNames, fieldNames...)
	return i
}

func (i InsertStmt) Values(values ...Expr) InsertStmt {
	i.values = append(i.values, values...)
	return i
}

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
