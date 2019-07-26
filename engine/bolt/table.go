package bolt

import (
	"github.com/asdine/genji/engine"
	"github.com/asdine/genji/field"
	"github.com/asdine/genji/record"
	"github.com/asdine/genji/table"
	bolt "github.com/etcd-io/bbolt"
)

// A Table is represented by a bucket.
// Each record is stored as a key value pair, where the recordID is stored as the key.
// Table uses the codec to encode the record and store is as the value.
type Table struct {
	bucket *bolt.Bucket
	codec  record.Codec
	tx     *bolt.Tx
	name   []byte
}

// Insert a record into the table bucket. If the record implements the table.Pker interface,
// it uses the returned value as the recordID. If not, it generates a recordID using NextSequence.
func (t *Table) Insert(r record.Record) (recordID []byte, err error) {
	if !t.bucket.Writable() {
		return nil, engine.ErrTransactionReadOnly
	}

	if pker, ok := r.(table.Pker); ok {
		recordID, err = pker.Pk()
		if err != nil {
			return nil, err
		}
	} else {
		seq, err := t.bucket.NextSequence()
		if err != nil {
			return nil, err
		}

		recordID = field.EncodeUint64(seq)
	}

	data, err := t.codec.Encode(r)
	if err != nil {
		return nil, err
	}

	err = t.bucket.Put(recordID, data)
	if err != nil {
		return nil, err
	}

	return recordID, nil
}

// Record returns a record by recordID. If not found, returns table.ErrRecordNotFound.
func (t *Table) Record(recordID []byte) (record.Record, error) {
	v := t.bucket.Get(recordID)
	if v == nil {
		return nil, table.ErrRecordNotFound
	}

	return t.codec.Decode(v)
}

// Delete a record by recordID. If not found, returns table.ErrRecordNotFound.
func (t *Table) Delete(recordID []byte) error {
	if !t.bucket.Writable() {
		return engine.ErrTransactionReadOnly
	}

	v := t.bucket.Get(recordID)
	if v == nil {
		return table.ErrRecordNotFound
	}

	return t.bucket.Delete(recordID)
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

// Replace a record by recordID. If not found, returns table.ErrRecordNotFound.
func (t *Table) Replace(recordID []byte, r record.Record) error {
	if !t.bucket.Writable() {
		return engine.ErrTransactionReadOnly
	}

	v := t.bucket.Get(recordID)
	if v == nil {
		return table.ErrRecordNotFound
	}

	v, err := t.codec.Encode(r)
	if err != nil {
		return err
	}

	return t.bucket.Put(recordID, v)
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
