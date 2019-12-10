package database

import (
	"bytes"

	"github.com/asdine/genji/document"
	"github.com/asdine/genji/document/encoding"
	"github.com/asdine/genji/engine"
	"github.com/asdine/genji/index"
	"github.com/pkg/errors"
)

// A Table represents a collection of records.
type Table struct {
	tx       *Transaction
	Store    engine.Store
	name     string
	CfgStore *tableConfigStore
}

type encodedRecordWithKey struct {
	encoding.EncodedDocument

	key []byte
}

func (e encodedRecordWithKey) Key() []byte {
	return e.key
}

// Iterate goes through all the records of the table and calls the given function by passing each one of them.
// If the given function returns an error, the iteration stops.
func (t *Table) Iterate(fn func(d document.Document) error) error {
	// To avoid unnecessary allocations, we create the slice once and reuse it
	// at each call of the fn method.
	// Since the AscendGreaterOrEqual is never supposed to call the callback concurrently
	// we can assume that it's thread safe.
	// TODO(asdine) Add a mutex if proven necessary
	var r encodedRecordWithKey

	return t.Store.AscendGreaterOrEqual(nil, func(k, v []byte) error {
		r.EncodedDocument = v
		r.key = k
		// r must be passed as pointer, not value, because passing a value to an interface
		// requires an allocation, while it doesn't for a pointer.
		return fn(&r)
	})
}

// GetRecord returns one record by key.
func (t *Table) GetRecord(key []byte) (document.Document, error) {
	v, err := t.Store.Get(key)
	if err != nil {
		if err == engine.ErrKeyNotFound {
			return nil, ErrRecordNotFound
		}
		return nil, errors.Wrapf(err, "failed to fetch record %q", key)
	}

	var r encodedRecordWithKey
	r.EncodedDocument = encoding.EncodedDocument(v)
	r.key = key
	return &r, err
}

func (t *Table) generateKey(d document.Document) ([]byte, error) {
	cfg, err := t.CfgStore.Get(t.name)
	if err != nil {
		return nil, err
	}

	var key []byte
	if cfg.PrimaryKeyName != "" {
		v, err := d.GetByField(cfg.PrimaryKeyName)
		if err != nil {
			return nil, err
		}
		cv, err := v.ConvertTo(cfg.PrimaryKeyType)
		if err != nil {
			return nil, err
		}
		return encoding.EncodeValue(cv)
	}

	t.tx.db.mu.Lock()
	defer t.tx.db.mu.Unlock()

	cfg, err = t.CfgStore.Get(t.name)
	if err != nil {
		return nil, err
	}

	cfg.lastKey++
	key = encoding.EncodeInt64(cfg.lastKey)
	err = t.CfgStore.Replace(t.name, cfg)
	if err != nil {
		return nil, err
	}

	return key, nil
}

// Insert the record into the table.
// If a primary key has been specified during the table creation, the field is expected to be present
// in the given document.
// If no primary key has been selected, a monotonic autoincremented integer key will be generated.
func (t *Table) Insert(d document.Document) ([]byte, error) {
	key, err := t.generateKey(d)
	if err != nil {
		return nil, err
	}

	_, err = t.Store.Get(key)
	if err == nil {
		return nil, ErrDuplicateRecord
	}

	v, err := encoding.EncodeDocument(d)
	if err != nil {
		return nil, errors.Wrap(err, "failed to encode record")
	}

	err = t.Store.Put(key, v)
	if err != nil {
		return nil, err
	}

	indexes, err := t.Indexes()
	if err != nil {
		return nil, err
	}

	for _, idx := range indexes {
		v, err := d.GetByField(idx.FieldName)
		if err != nil {
			v = document.NewNullValue()
		}

		err = idx.Set(v, key)
		if err != nil {
			if err == index.ErrDuplicate {
				return nil, ErrDuplicateRecord
			}

			return nil, err
		}
	}

	return key, nil
}

// Delete a record by key.
// Indexes are automatically updated.
func (t *Table) Delete(key []byte) error {
	r, err := t.GetRecord(key)
	if err != nil {
		return err
	}

	indexes, err := t.Indexes()
	if err != nil {
		return err
	}

	for _, idx := range indexes {
		v, err := r.GetByField(idx.FieldName)
		if err != nil {
			return err
		}

		err = idx.Delete(v, key)
		if err != nil {
			return err
		}
	}

	return t.Store.Delete(key)
}

// Replace a record by key.
// An error is returned if the key doesn't exist.
// Indexes are automatically updated.
func (t *Table) Replace(key []byte, d document.Document) error {
	indexes, err := t.Indexes()
	if err != nil {
		return err
	}

	return t.replace(indexes, key, d)
}

func (t *Table) replace(indexes map[string]Index, key []byte, d document.Document) error {
	// make sure key exists
	old, err := t.GetRecord(key)
	if err != nil {
		return err
	}

	// remove key from indexes
	for _, idx := range indexes {
		v, err := old.GetByField(idx.FieldName)
		if err != nil {
			return err
		}

		err = idx.Delete(v, key)
		if err != nil {
			return err
		}
	}

	// encode new record
	v, err := encoding.EncodeDocument(d)
	if err != nil {
		return errors.Wrap(err, "failed to encode record")
	}

	// replace old record with new record
	err = t.Store.Put(key, v)
	if err != nil {
		return err
	}

	// update indexes
	for _, idx := range indexes {
		v, err := d.GetByField(idx.FieldName)
		if err != nil {
			continue
		}

		err = idx.Set(v, key)
		if err != nil {
			return err
		}
	}

	return err
}

// Truncate deletes all the records from the table.
func (t *Table) Truncate() error {
	return t.Store.Truncate()
}

// TableName returns the name of the table.
func (t *Table) TableName() string {
	return t.name
}

// Indexes returns a map of all the indexes of a table.
func (t *Table) Indexes() (map[string]Index, error) {
	s, err := t.tx.tx.Store(indexStoreName)
	if err != nil {
		return nil, err
	}

	tb := Table{
		tx:    t.tx,
		Store: s,
		name:  indexStoreName,
	}

	tableName := []byte(t.name)
	indexes := make(map[string]Index)

	err = document.NewStream(&tb).
		Filter(func(d document.Document) (bool, error) {
			v, err := d.GetByField("TableName")
			if err != nil {
				return false, err
			}

			b, err := v.ConvertToBytes()
			if err != nil {
				return false, err
			}

			return bytes.Equal(b, tableName), nil
		}).
		Iterate(func(d document.Document) error {
			var opts indexOptions
			err := opts.ScanDocument(d)
			if err != nil {
				return err
			}

			indexes[opts.FieldName] = Index{
				Index: index.New(t.tx.tx, index.Options{
					IndexName: opts.IndexName,
					TableName: opts.TableName,
					FieldName: opts.FieldName,
					Unique:    opts.Unique,
				}),
				IndexName: opts.IndexName,
				TableName: opts.TableName,
				FieldName: opts.FieldName,
				Unique:    opts.Unique,
			}

			return nil
		})
	if err != nil {
		return nil, err
	}

	return indexes, nil
}
