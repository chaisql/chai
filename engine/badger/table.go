package badger

import (
	"github.com/asdine/genji/record"
	"github.com/dgraph-io/badger"
)

type Table struct {
	tx     *badger.Txn
	prefix []byte
}

func (t *Table) Insert(r record.Record) (rowid []byte, err error) {
	return
}

func (t *Table) Record(rowid []byte) (record.Record, error) {
	return nil, nil
}

func (t *Table) Delete(rowid []byte) error {
	return nil
}

func (t *Table) Iterate(fn func([]byte, record.Record) error) error {
	return nil
}

func (t *Table) Replace(rowid []byte, r record.Record) error {
	return nil
}
