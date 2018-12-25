package query

import (
	"github.com/asdine/genji/engine"
	"github.com/asdine/genji/record"
)

type TableReader struct {
	engine.TableReader

	err error
}

func NewTableReader(t engine.TableReader) TableReader {
	return TableReader{
		TableReader: t,
	}
}

func (t TableReader) Err() error {
	return t.err
}

func (t TableReader) ForEach(fn func(record.Record) error) TableReader {
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

func (t TableReader) Filter(fn func(record.Record) (bool, error)) TableReader {
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

func (t TableReader) Map(fn func(record.Record) (record.Record, error)) TableReader {
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
