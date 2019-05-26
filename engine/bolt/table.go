package bolt

import (
	"github.com/asdine/genji/engine"
	"github.com/asdine/genji/field"
	"github.com/asdine/genji/record"
	"github.com/asdine/genji/table"
	bolt "github.com/etcd-io/bbolt"
)

// A Table is represented by a bucket.
// Each record is stored as a key value pair, where the rowid is stored as the key.
// Table uses the codec to encode the record and store is as the value.
type Table struct {
	bucket *bolt.Bucket
	codec  record.Codec
	tx     *bolt.Tx
	name   []byte
}

// Insert a record into the table bucket. If the record implements the table.Pker interface,
// it uses the returned value as the rowid. If not, it generates a rowid using NextSequence.
func (t *Table) Insert(r record.Record) (rowid []byte, err error) {
	if !t.bucket.Writable() {
		return nil, engine.ErrTransactionReadOnly
	}

	if pker, ok := r.(table.Pker); ok {
		rowid, err = pker.Pk()
		if err != nil {
			return nil, err
		}
	} else {
		seq, err := t.bucket.NextSequence()
		if err != nil {
			return nil, err
		}

		rowid = field.EncodeUint64(seq)
	}

	data, err := t.codec.Encode(r)
	if err != nil {
		return nil, err
	}

	err = t.bucket.Put(rowid, data)
	if err != nil {
		return nil, err
	}

	return rowid, nil
}

// Record returns a record by rowid. If not found, returns table.ErrRecordNotFound.
func (t *Table) Record(rowid []byte) (record.Record, error) {
	v := t.bucket.Get(rowid)
	if v == nil {
		return nil, table.ErrRecordNotFound
	}

	return t.codec.Decode(v)
}

// Delete a record by rowid. If not found, returns table.ErrRecordNotFound.
func (t *Table) Delete(rowid []byte) error {
	if !t.bucket.Writable() {
		return engine.ErrTransactionReadOnly
	}

	v := t.bucket.Get(rowid)
	if v == nil {
		return table.ErrRecordNotFound
	}

	return t.bucket.Delete(rowid)
}

// Iterate through all the records of the table until the end or until fn
// returns an error.
func (t *Table) Iterate(fn func([]byte, record.Record) error) error {
	return t.bucket.ForEach(func(k, v []byte) error {
		if v == nil {
			return nil
		}

		r, err := t.codec.Decode(v)
		if err != nil {
			return err
		}

		return fn(k, r)
	})
}

// Replace a record by rowid. If not found, returns table.ErrRecordNotFound.
func (t *Table) Replace(rowid []byte, r record.Record) error {
	if !t.bucket.Writable() {
		return engine.ErrTransactionReadOnly
	}

	v := t.bucket.Get(rowid)
	if v == nil {
		return table.ErrRecordNotFound
	}

	v, err := t.codec.Encode(r)
	if err != nil {
		return err
	}

	return t.bucket.Put(rowid, v)
}

// Truncate deletes all the records of the table.
func (t *Table) Truncate() error {
	if !t.bucket.Writable() {
		return engine.ErrTransactionReadOnly
	}

	err := t.tx.DeleteBucket(t.name)
	if err != nil {
		return err
	}

	_, err = t.tx.CreateBucket(t.name)
	return err
}
