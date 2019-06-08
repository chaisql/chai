package query

import (
	"errors"

	"github.com/asdine/genji"
	"github.com/asdine/genji/engine"
	"github.com/asdine/genji/record"
	"github.com/asdine/genji/table"
)

type Query struct {
	fieldSelectors []FieldSelector
	tableSelector  TableSelector
	whereExpr      Expr
}

func Select(selectors ...FieldSelector) Query {
	return Query{fieldSelectors: selectors}
}

func (q Query) Run(tx *genji.Tx) Result {
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

func (q Query) Where(e Expr) Query {
	q.whereExpr = e
	return q
}

func (q Query) From(selector TableSelector) Query {
	q.tableSelector = selector
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
