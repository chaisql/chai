package query

import (
	"errors"

	"github.com/asdine/genji/engine"
	"github.com/asdine/genji/record"
	"github.com/asdine/genji/table"
)

type Query struct {
	fieldSelectors []FieldSelector
	tableSelector  TableSelector
	matchers       []Matcher
}

func Select(selectors ...FieldSelector) Query {
	return Query{fieldSelectors: selectors}
}

func (q Query) Run(tx engine.Transaction) (table.Reader, error) {
	if q.tableSelector == nil {
		return nil, errors.New("missing table selector")
	}

	t, err := q.tableSelector.SelectTable(tx)
	if err != nil {
		return nil, err
	}

	matcher := And(q.matchers...)
	tree, err := matcher.MatchIndex(q.tableSelector.Name(), tx)
	if err != nil && err != engine.ErrIndexNotFound {
		return nil, err
	}

	var b table.Browser

	if err == nil && tree.Len() > 0 {
		b.Reader = &indexResultTable{
			tree:  tree,
			table: t,
		}
	} else {
		b = table.NewBrowser(t).
			Filter(func(r record.Record) (bool, error) {
				return matcher.Match(r)
			})
	}

	b = b.Map(func(r record.Record) (record.Record, error) {
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
		return nil, b.Err()
	}

	return b.Reader, nil
}

func (q Query) Where(matchers ...Matcher) Query {
	q.matchers = append(q.matchers, matchers...)
	return q
}

func (q Query) From(selector TableSelector) Query {
	q.tableSelector = selector
	return q
}
