package query

import (
	"github.com/asdine/genji/engine"
	"github.com/asdine/genji/record"
)

type Query struct {
	t       engine.TableReader
	actions []func([]engine.TableReader) ([]engine.TableReader, error)
	err     error
}

func (q *Query) Run() ([]engine.TableReader, error) {
	var err error
	g := []engine.TableReader{q.t}

	for _, action := range q.actions {
		g, err = action(g)
		if err != nil {
			return nil, err
		}
	}

	return g, nil
}

func (q *Query) ForEach(fn func(record.Record) error) *Query {
	action := func(groups []engine.TableReader) ([]engine.TableReader, error) {
		for _, t := range groups {
			c := t.Cursor()

			for c.Next() {
				if err := c.Err(); err != nil {
					return nil, err
				}

				r := c.Record()

				err := fn(r)
				if err != nil {
					return nil, err
				}
			}
		}

		return groups, nil
	}

	q.actions = append(q.actions, action)
	return q
}

func (q *Query) Filter(fn func(record.Record) (bool, error)) *Query {
	action := func(groups []engine.TableReader) ([]engine.TableReader, error) {
		g := make([]engine.TableReader, len(groups))

		for i, t := range groups {
			var rb RecordBuffer

			c := t.Cursor()

			for c.Next() {
				if err := c.Err(); err != nil {
					return nil, err
				}

				r := c.Record()

				ok, err := fn(r)
				if err != nil {
					return nil, err
				}

				if ok {
					rb.Add(r)
				}
			}

			g[i] = rb
		}

		return groups, nil
	}

	q.actions = append(q.actions, action)
	return q
}

func (q *Query) Map(fn func(record.Record) (record.Record, error)) *Query {
	action := func(groups []engine.TableReader) ([]engine.TableReader, error) {
		g := make([]engine.TableReader, len(groups))

		for i, t := range groups {
			var rb RecordBuffer

			c := t.Cursor()

			for c.Next() {
				if err := c.Err(); err != nil {
					return nil, err
				}

				r, err := fn(c.Record())
				if err != nil {
					return nil, err
				}

				rb.Add(r)
			}

			g[i] = rb
		}

		return groups, nil
	}

	q.actions = append(q.actions, action)
	return q
}
