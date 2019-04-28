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

	err = t.txn.Set(makeRecordKey(t.prefix, rowid), data)
	if err != nil {
		return nil, err
	}

	return rowid, nil
}

func makeRecordKey(prefix, rowid []byte) []byte {
	key := make([]byte, 0, len(prefix)+1+len(rowid))
	key = append(key, prefix...)
	key = append(key, separator)
	key = append(key, rowid...)
	return key
}

func (t *Table) Record(rowid []byte) (record.Record, error) {
	it, err := t.txn.Get(makeRecordKey(t.prefix, rowid))
	if err != nil {
		if err == badger.ErrKeyNotFound {
			return nil, table.ErrRecordNotFound
		}

		return nil, err
	}

	v, err := it.ValueCopy(nil)
	if err != nil {
		return nil, err
	}

	return t.codec.Decode(v)
}

func (t *Table) Delete(rowid []byte) error {
	if !t.writable {
		return engine.ErrTransactionReadOnly
	}

	key := makeRecordKey(t.prefix, rowid)
	_, err := t.txn.Get(key)
	if err != nil {
		if err == badger.ErrKeyNotFound {
			return table.ErrRecordNotFound
		}

		return err
	}

	return t.txn.Delete(key)
}

func (t *Table) Iterate(fn func([]byte, record.Record) error) error {
	opt := badger.DefaultIteratorOptions
	opt.PrefetchSize = 10
	it := t.txn.NewIterator(opt)
	defer it.Close()

	for it.Rewind(); it.Valid(); it.Next() {
		item := it.Item()

		v, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}

		r, err := t.codec.Decode(v)
		if err != nil {
			return err
		}

		err = fn(item.Key(), r)
		if err != nil {
			return err
		}
	}

	return nil
}

func (t *Table) Replace(rowid []byte, r record.Record) error {
	return nil
}
