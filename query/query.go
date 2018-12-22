package query

import (
	"github.com/asdine/genji/engine"
	"github.com/asdine/genji/field"
	"github.com/asdine/genji/record"
)

type Query struct {
	actions    []func([]engine.TableReader) ([]engine.TableReader, error)
	aggregator Aggregator
	tq         *TableQuery
}

func (q Query) Run(t engine.TableReader) (engine.TableReader, error) {
	var err error

	group := []engine.TableReader{t}
	for _, action := range q.actions {
		group, err = action(group)
		if err != nil {
			return nil, err
		}
	}

	if q.aggregator == nil {
		q.aggregator = aggregate
	}

	t, err = q.aggregator(group)
	if err != nil {
		return nil, err
	}

	if q.tq != nil {
		return q.tq.Run(t)
	}

	return t, nil
}

func (q Query) ForEach(fn func(record.Record) error) Query {
	action := func(groups []engine.TableReader) ([]engine.TableReader, error) {
		for _, t := range groups {
			tr := NewTableReader(t)

			if err := tr.ForEach(fn).Err(); err != nil {
				return nil, err
			}
		}

		return groups, nil
	}

	q.actions = append(q.actions, action)
	return q
}

func (q Query) Filter(fn func(record.Record) (bool, error)) Query {
	action := func(groups []engine.TableReader) ([]engine.TableReader, error) {
		g := make([]engine.TableReader, len(groups))

		for i, t := range groups {
			tr := NewTableReader(t).Filter(fn)
			if err := tr.Err(); err != nil {
				return nil, err
			}

			g[i] = tr.TableReader
		}

		return g, nil
	}

	q.actions = append(q.actions, action)
	return q
}

func (q Query) Map(fn func(record.Record) (record.Record, error)) Query {
	action := func(groups []engine.TableReader) ([]engine.TableReader, error) {
		g := make([]engine.TableReader, len(groups))

		for i, t := range groups {
			tr := NewTableReader(t).Map(fn)
			if err := tr.Err(); err != nil {
				return nil, err
			}

			g[i] = tr.TableReader
		}

		return g, nil
	}

	q.actions = append(q.actions, action)
	return q
}

func (q Query) Count(fieldName string) *TableQuery {
	var tq TableQuery

	q.aggregator = func(groups []engine.TableReader) (engine.TableReader, error) {
		var rb engine.RecordBuffer

		for _, t := range groups {
			var counter int

			var fb record.FieldBuffer

			tr := NewTableReader(t).ForEach(func(r record.Record) error {
				f, err := r.Field(fieldName)
				if err != nil {
					return err
				}

				if f.Data == nil {
					return nil
				}

				counter++
				// TODO: add helpers for fields
				c := r.Cursor()
				for c.Next() {
					if c.Err() != nil {
						return c.Err()
					}

					f := c.Field()
					if err != nil {
						return err
					}

					fb.Add(f)
				}

				fb.Add(field.NewInt64("count("+fieldName+")", int64(counter)))
				return nil
			})

			if tr.Err() != nil {
				return nil, tr.Err()
			}

			rb.Add(&fb)
		}

		return &rb, nil
	}

	q.tq = &tq

	return &tq
}

type Aggregator func([]engine.TableReader) (engine.TableReader, error)

func aggregate(groups []engine.TableReader) (engine.TableReader, error) {
	var rb engine.RecordBuffer

	for _, t := range groups {
		tr := NewTableReader(t).ForEach(func(r record.Record) error {
			rb.Add(r)
			return nil
		})

		if tr.Err() != nil {
			return nil, tr.Err()
		}
	}

	return &rb, nil
}

type TableQuery struct {
	actions []func(*TableReader) (*TableReader, error)
}

func (tq *TableQuery) Run(t engine.TableReader) (engine.TableReader, error) {
	var err error
	tr := NewTableReader(t)

	for _, action := range tq.actions {
		tr, err = action(tr)
		if err != nil {
			return nil, err
		}
	}

	return tr.TableReader, nil
}

func (tq *TableQuery) Filter(fn func(record.Record) (bool, error)) *TableQuery {
	tq.actions = append(tq.actions, func(t *TableReader) (*TableReader, error) {
		return t.Filter(fn), nil
	})

	return tq
}

func (tq *TableQuery) Map(fn func(record.Record) (record.Record, error)) *TableQuery {
	tq.actions = append(tq.actions, func(t *TableReader) (*TableReader, error) {
		return t.Map(fn), nil
	})

	return tq
}

type TableReader struct {
	engine.TableReader

	err error
}

func NewTableReader(t engine.TableReader) *TableReader {
	return &TableReader{
		TableReader: t,
	}
}

func (t *TableReader) Err() error {
	return t.err
}

func (t *TableReader) ForEach(fn func(record.Record) error) *TableReader {
	if t.err != nil {
		return t
	}

	c := t.Cursor()

	for c.Next() {
		if err := c.Err(); err != nil {
			t.err = err
			return t
		}

		err := fn(c.Record())
		if err != nil {
			t.err = err
			return t
		}
	}

	return t
}

func (t *TableReader) Filter(fn func(record.Record) (bool, error)) *TableReader {
	var rb engine.RecordBuffer

	t.ForEach(func(r record.Record) error {
		ok, err := fn(r)
		if err != nil {
			return err
		}

		if ok {
			rb.Add(r)
		}

		return nil
	})

	if t.err != nil {
		return t
	}

	return &TableReader{TableReader: &rb}
}

func (t *TableReader) Map(fn func(record.Record) (record.Record, error)) *TableReader {
	var rb engine.RecordBuffer

	t.ForEach(func(r record.Record) error {
		r, err := fn(r)
		if err != nil {
			return err
		}

		rb.Add(r)
		return nil
	})

	if t.err != nil {
		return t
	}

	return &TableReader{TableReader: &rb}
}
