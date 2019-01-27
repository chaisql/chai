package query

import (
	"github.com/asdine/genji/record"
	"github.com/asdine/genji/table"
)

type Query struct {
	selectors []FieldSelector
	matchers  []Matcher
}

func Select(selectors ...FieldSelector) Query {
	return Query{selectors: selectors}
}

type FieldSelector interface {
	Name() string
}

func (q Query) Run(t table.Reader) (table.Reader, error) {
	matcher := And(q.matchers...)

	b := table.NewBrowser(t).
		Filter(func(r record.Record) (bool, error) {
			return matcher.Match(r)
		}).
		Map(func(r record.Record) (record.Record, error) {
			var fb record.FieldBuffer

			for _, s := range q.selectors {
				f, err := r.Field(s.Name())
				if err != nil {
					return nil, err
				}

				fb.Add(f)
			}

			return &fb, nil
		})

	if b.Err() != nil {
		return nil, b.Err()
	}

	return b.Reader, nil
}

func (q Query) Where(matchers ...Matcher) Query {
	q.matchers = append(q.matchers, matchers...)
	return q
}
