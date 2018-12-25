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

func (r Reader) Err() error {
	return r.err
}

func (r Reader) ForEach(fn func(record.Record) error) Reader {
	if r.err != nil {
		return r
	}

	c := r.Cursor()

	for c.Next() {
		if err := c.Err(); err != nil {
			r.err = err
			return r
		}

		err := fn(c.Record())
		if err != nil {
			r.err = err
			return r
		}
	}

	return r
}

func (r Reader) Filter(fn func(record.Record) (bool, error)) Reader {
	var rb engine.RecordBuffer

	r = r.ForEach(func(r record.Record) error {
		ok, err := fn(r)
		if err != nil {
			return err
		}

		if ok {
			rb.Add(r)
		}

		return nil
	})

	if r.err == nil {
		r.TableReader = &rb
	}

	return r
}

func (r Reader) Map(fn func(record.Record) (record.Record, error)) Reader {
	var rb engine.RecordBuffer

	r = r.ForEach(func(r record.Record) error {
		r, err := fn(r)
		if err != nil {
			return err
		}

		rb.Add(r)
		return nil
	})

	if r.err == nil {
		r.TableReader = rb
	}

	return r
}

func (r Reader) GroupBy(fieldName string) GroupReader {
	var g GroupReader

	if r.err != nil {
		g.err = r.err
		return g
	}

	m := make(map[string]*engine.RecordBuffer)
	var values []string

	tr := r.ForEach(func(r record.Record) error {
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

func (r Reader) Count() (int, error) {
	if r.err != nil {
		return 0, r.err
	}

	counter := 0
	r = r.ForEach(func(r record.Record) error {
		counter++
		return nil
	})

	return counter, r.err
}

type GroupReader struct {
	Readers []Reader
	err     error
}

func (g GroupReader) Err() error {
	return g.err
}

func (g GroupReader) Concat() Reader {
	var r Reader

	if g.err != nil {
		r.err = g.err
		return r
	}

	var fb engine.RecordBuffer

	for _, r := range g.Readers {
		err := fb.AddFrom(r)
		if err != nil {
			r.err = err
			return r
		}
	}

	r.TableReader = &fb
	return r
}
