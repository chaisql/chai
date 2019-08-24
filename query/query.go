package query

import (
	"errors"
	"fmt"

	"github.com/asdine/genji"
	"github.com/asdine/genji/field"
	"github.com/asdine/genji/record"
	"github.com/asdine/genji/table"
)

// Result of a query.
type Result struct {
	*table.Stream
	err error
}

// Err returns a non nil error if an error occured during the query.
func (r Result) Err() error {
	return r.err
}

// Scan takes a table scanner and passes it the result table.
func (r Result) Scan(s table.Scanner) error {
	if r.err != nil {
		return r.err
	}

	return s.ScanTable(r.Stream)
}

// SelectStmt is a DSL that allows creating a full Select query.
// It is typically created using the Select function.
type SelectStmt struct {
	tableSelector TableSelector
	whereExpr     Expr
	offsetExpr    Expr
	limitExpr     Expr
}

// Select creates a DSL equivalent to the SQL Select command.
func Select() SelectStmt {
	return SelectStmt{}
}

// Run the Select query within tx.
// If Where was called, records will be filtered depending on the result of the
// given expression. If the Where expression implements the IndexMatcher interface,
// the MatchIndex method will be called instead of the Eval one.
func (q SelectStmt) Run(tx *genji.Tx) Result {
	if q.tableSelector == nil {
		return Result{err: errors.New("missing table selector")}
	}

	offset := -1
	limit := -1

	if q.offsetExpr != nil {
		s, err := q.offsetExpr.Eval(EvalContext{
			Tx: tx,
		})
		if err != nil {
			return Result{err: err}
		}
		if s.Type < field.Int {
			return Result{err: fmt.Errorf("offset expression must evaluate to a 64 bit integer, got %q", s.Type)}
		}
		offset, err = field.DecodeInt(s.Data)
		if err != nil {
			return Result{err: err}
		}
	}

	if q.limitExpr != nil {
		s, err := q.limitExpr.Eval(EvalContext{
			Tx: tx,
		})
		if err != nil {
			return Result{err: err}
		}
		if s.Type < field.Int {
			return Result{err: fmt.Errorf("limit expression must evaluate to a 64 bit integer, got %q", s.Type)}
		}
		limit, err = field.DecodeInt(s.Data)
		if err != nil {
			return Result{err: err}
		}
	}

	t, err := q.tableSelector.SelectTable(tx)
	if err != nil {
		return Result{err: err}
	}

	var tr table.Reader = t

	var useIndex bool
	if im, ok := q.whereExpr.(IndexMatcher); ok {
		tree, ok, err := im.MatchIndex(t)
		if err != nil && err != genji.ErrIndexNotFound {
			return Result{err: err}
		}

		if ok && err == nil {
			useIndex = true
			tr = &indexResultTable{
				tree:  tree,
				table: t,
			}
		}
	}

	st := table.NewStream(tr)

	if !useIndex {
		st = st.Filter(whereClause(tx, q.whereExpr))
	}

	if offset > 0 {
		st = st.Offset(offset)
	}

	if limit >= 0 {
		st = st.Limit(limit)
	}

	return Result{Stream: &st}
}

// Where uses e to filter records if it evaluates to a falsy value.
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

// Limit the number of records returned.
func (q SelectStmt) Limit(offset int) SelectStmt {
	q.limitExpr = Int64Value(int64(offset))
	return q
}

// LimitExpr takes an expression that will be evaluated to determine
// how many records the query must return.
// The result of the evaluation must be an integer.
func (q SelectStmt) LimitExpr(e Expr) SelectStmt {
	q.limitExpr = e
	return q
}

// Offset indicates the number of records to skip.
func (q SelectStmt) Offset(offset int) SelectStmt {
	q.offsetExpr = Int64Value(int64(offset))
	return q
}

// OffsetExpr takes an expression that will be evaluated to determine
// how many records the query must skip.
// The result of the evaluation must be a field.Int64.
func (q SelectStmt) OffsetExpr(e Expr) SelectStmt {
	q.offsetExpr = e
	return q
}

var errStop = errors.New("stop")

func whereClause(tx *genji.Tx, e Expr) func(recordID []byte, r record.Record) (bool, error) {
	if e == nil {
		return func(recordID []byte, r record.Record) (bool, error) {
			return true, nil
		}
	}

	return func(recordID []byte, r record.Record) (bool, error) {
		sc, err := e.Eval(EvalContext{Tx: tx, Record: r})
		if err != nil {
			return false, err
		}

		return sc.Truthy(), nil
	}
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

	var useIndex bool
	var tr table.Reader = t

	if im, ok := d.whereExpr.(IndexMatcher); ok {
		tree, ok, err := im.MatchIndex(t)
		if err != nil && err != genji.ErrIndexNotFound {
			return err
		}

		if ok && err == nil {
			useIndex = true
			tr = &indexResultTable{
				tree:  tree,
				table: t,
			}
		}
	}

	st := table.NewStream(tr)

	if !useIndex {
		st = st.Filter(whereClause(tx, d.whereExpr))
	}

	return st.Iterate(func(recordID []byte, r record.Record) error {
		return t.Delete(recordID)
	})
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
// If the Fields method was called prior to the Run method, each value will be associated with one of the given field name, in order.
// If the Fields method wasn't called, this will return an error
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

	recordID, err := t.Insert(&fb)
	if err != nil {
		return Result{err: err}
	}

	st := table.NewStream(table.NewReaderFromRecords(record.FieldBuffer([]field.Field{
		field.NewBytes("recordID", recordID),
	})))
	return Result{Stream: &st}
}

// UpdateStmt is a DSL that allows creating a full Update query.
// It is typically created using the Update function.
type UpdateStmt struct {
	tableSelector TableSelector
	pairs         map[string]Expr
	whereExpr     Expr
}

// Update creates a DSL equivalent to the SQL Update command.
func Update(tableSelector TableSelector) UpdateStmt {
	return UpdateStmt{
		tableSelector: tableSelector,
		pairs:         make(map[string]Expr),
	}
}

// Set assignes the result of the evaluation of e into the field selected
// by f.
func (u UpdateStmt) Set(fieldName string, e Expr) UpdateStmt {
	u.pairs[fieldName] = e
	return u
}

// Where uses e to filter records if it evaluates to a falsy value.
// Calling this method is optional.
func (u UpdateStmt) Where(e Expr) UpdateStmt {
	u.whereExpr = e
	return u
}

// Run the Update query within tx.
// If Where was called, records will be filtered depending on the result of the
// given expression. If the Where expression implements the IndexMatcher interface,
// the MatchIndex method will be called instead of the Eval one.
func (u UpdateStmt) Run(tx *genji.Tx) error {
	if u.tableSelector == nil {
		return errors.New("missing table selector")
	}

	if len(u.pairs) == 0 {
		return errors.New("Set method not called")
	}

	t, err := u.tableSelector.SelectTable(tx)
	if err != nil {
		return err
	}

	var tr table.Reader = t

	var useIndex bool

	if im, ok := u.whereExpr.(IndexMatcher); ok {
		tree, ok, err := im.MatchIndex(t)
		if err != nil && err != genji.ErrIndexNotFound {
			return err
		}

		if ok && err == nil {
			useIndex = true
			tr = &indexResultTable{
				tree:  tree,
				table: t,
			}
		}
	}

	st := table.NewStream(tr)

	if !useIndex {
		st = st.Filter(whereClause(tx, u.whereExpr))
	}

	return st.Iterate(func(recordID []byte, r record.Record) error {
		var fb record.FieldBuffer
		err := fb.ScanRecord(r)
		if err != nil {
			return err
		}

		for fname, e := range u.pairs {
			f, err := fb.GetField(fname)
			if err != nil {
				return err
			}

			s, err := e.Eval(EvalContext{
				Tx:     tx,
				Record: r,
			})
			if err != nil {
				return err
			}

			f.Type = s.Type
			f.Data = s.Data
			err = fb.Replace(f.Name, f)
			if err != nil {
				return err
			}

			err = t.Replace(recordID, &fb)
			if err != nil {
				return err
			}
		}

		return nil
	})
}
