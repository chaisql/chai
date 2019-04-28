package badger

import (
	"github.com/asdine/genji/engine"
	"github.com/asdine/genji/field"
	"github.com/asdine/genji/record"
	"github.com/asdine/genji/table"
	"github.com/dgraph-io/badger"
)

type Table struct {
	txn      *badger.Txn
	prefix   []byte
	writable bool
	seq      *badger.Sequence
	codec    record.Codec
}

func (t *Table) Insert(r record.Record) (rowid []byte, err error) {
	if !t.writable {
		return nil, engine.ErrTransactionReadOnly
	}

	if pker, ok := r.(table.Pker); ok {
		rowid, err = pker.Pk()
		if err != nil {
			return nil, err
		}
	} else {
		seq, err := t.seq.Next()
		if err != nil {
			return nil, err
		}

		// TODO(asdine): encode in uint64 if that makes sense.
		rowid = field.EncodeInt64(int64(seq))
	}

	data, err := t.codec.Encode(r)
	if err != nil {
		return nil, err
	}

	key := make([]byte, 0, len(t.prefix)+1+len(rowid))
	key = append(key, t.prefix...)
	key = append(key, separator)
	key = append(key, rowid...)

	err = t.txn.Set(key, data)
	if err != nil {
		return nil, err
	}

	return rowid, nil
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
