package table

import (
	"sort"

	"github.com/asdine/genji/engine"
	"github.com/asdine/genji/record"
)

type Reader struct {
	engine.TableReader

	err error
}

func NewReader(t engine.TableReader) Reader {
	return Reader{
		TableReader: t,
	}
}

func (t Reader) Err() error {
	return t.err
}

func (t Reader) ForEach(fn func(record.Record) error) Reader {
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

func (t Reader) Filter(fn func(record.Record) (bool, error)) Reader {
	var rb engine.RecordBuffer

	t = t.ForEach(func(r record.Record) error {
		ok, err := fn(r)
		if err != nil {
			return err
		}

		if ok {
			rb.Add(r)
		}

		return nil
	})

	if t.err == nil {
		t.TableReader = &rb
	}

	return t
}

func (t Reader) Map(fn func(record.Record) (record.Record, error)) Reader {
	var rb engine.RecordBuffer

	t = t.ForEach(func(r record.Record) error {
		r, err := fn(r)
		if err != nil {
			return err
		}

		rb.Add(r)
		return nil
	})

	if t.err == nil {
		t.TableReader = rb
	}

	return t
}

func (t Reader) GroupBy(fieldName string) GroupReader {
	var g GroupReader

	if t.err != nil {
		g.err = t.err
		return g
	}

	m := make(map[string]*engine.RecordBuffer)
	var values []string

	tr := t.ForEach(func(r record.Record) error {
		f, err := r.Field(fieldName)
		if err != nil {
			return err
		}

		k := string(f.Data)
		tr, ok := m[k]
		if !ok {
			tr = new(engine.RecordBuffer)
			m[k] = tr
			values = append(values, k)
		}

		tr.Add(r)
		return nil
	})

	if err := tr.Err(); err != nil {
		g.err = err
		return g
	}

	sort.Strings(values)

	for _, v := range values {
		g.Readers = append(g.Readers, NewReader(m[v]))
	}

	return g
}

func (t Reader) Count() (int, error) {
	if t.err != nil {
		return 0, t.err
	}

	counter := 0
	t = t.ForEach(func(r record.Record) error {
		counter++
		return nil
	})

	return counter, t.err
}

type GroupReader struct {
	Readers []Reader
	err     error
}

func (g GroupReader) Err() error {
	return g.err
}
