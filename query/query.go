package query

import (
	"errors"

	"github.com/asdine/genji"
	"github.com/asdine/genji/engine"
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

type Query struct {
	fieldSelectors []FieldSelector
	tableSelector  TableSelector
	matchers       []Matcher
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

	matcher := And(q.matchers...)
	tree, ok, err := matcher.MatchIndex(q.tableSelector.Name(), tx)
	if err != nil && err != engine.ErrIndexNotFound {
		return Result{err: err}
	}

	var b table.Browser

	if ok && err != nil {
		b.Reader = &indexResultTable{
			tree:  tree,
			table: t,
		}
	} else {
		b = table.NewBrowser(t).
			Filter(func(rowid []byte, r record.Record) (bool, error) {
				return matcher.Match(r)
			})
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

func (q Query) Where(matchers ...Matcher) Query {
	q.matchers = append(q.matchers, matchers...)
	return q
}

func (q Query) From(selector TableSelector) Query {
	q.tableSelector = selector
	return q
}
