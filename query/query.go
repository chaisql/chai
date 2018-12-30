package query

import (
	"github.com/asdine/genji/record"
	"github.com/asdine/genji/table"
)

type Query struct {
	selectors []FieldSelector
}

func Select(selectors ...FieldSelector) Query {
	return Query{selectors: selectors}
}

type FieldSelector interface {
	Name() string
}

func (q Query) Run(t table.Reader) (table.Reader, error) {
	var rb table.RecordBuffer

	tb := table.NewBrowser(t)
	tb = tb.ForEach(func(r record.Record) error {
		var fb record.FieldBuffer

		for _, s := range q.selectors {
			f, err := r.Field(s.Name())
			if err != nil {
				return err
			}

			fb.Add(f)
		}

		rb.Add(&fb)
		return nil
	})

	if tb.Err() != nil {
		return nil, tb.Err()
	}

	return &rb, nil
}
