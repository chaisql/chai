package table

import (
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

func (t Reader) Count() (int, error) {
	counter := 0
	t = t.ForEach(func(r record.Record) error {
		counter++
		return nil
	})

	return counter, t.err
}

type GroupReader []Reader
