package query

import (
	"github.com/asdine/genji"
	"github.com/asdine/genji/record"
	"github.com/asdine/genji/table"
)

type streamTable struct {
	r     table.Reader
	tx    *genji.Tx
	funcs []func() func(r record.Record) (record.Record, error)
}

func (t streamTable) Iterate(fn func(recordID []byte, r record.Record) error) error {
	funcs := make([]func(r record.Record) (record.Record, error), len(t.funcs))

	for i := range t.funcs {
		funcs[i] = t.funcs[i]()
	}

	err := t.r.Iterate(func(recordID []byte, r record.Record) error {
		var err error

		for _, sfn := range funcs {
			r, err = sfn(r)
			if err != nil {
				return err
			}
			if r == nil {
				return nil
			}
		}

		if r != nil {
			return fn(recordID, r)
		}

		return nil
	})
	if err == errStop {
		return nil
	}

	return err
}

func (streamTable) Record(recordID []byte) (record.Record, error) {
	return nil, nil
}
