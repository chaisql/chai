package badger

import (
	"github.com/asdine/genji/engine"
	"github.com/asdine/genji/field"
	"github.com/asdine/genji/record"
	"github.com/dgraph-io/badger"
)

type Table struct {
	txn      *badger.Txn
	prefix   []byte
	writable bool
	seq      *badger.Sequence
}

func (t *Table) Insert(r record.Record) (rowid []byte, err error) {
	if !t.writable {
		return nil, engine.ErrTransactionReadOnly
	}

	seq, err := t.seq.Next()
	if err != nil {
		return nil, err
	}
	rowid := field.EncodeInt64(int64(seq))

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
