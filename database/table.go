package database

import (
	"bytes"

	"github.com/asdine/genji/engine"
	"github.com/asdine/genji/index"
	"github.com/asdine/genji/document"
	"github.com/asdine/genji/value"
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
	document.EncodedRecord

	key []byte
}

func (e encodedRecordWithKey) Key() []byte {
	return e.key
}

// Iterate goes through all the records of the table and calls the given function by passing each one of them.
// If the given function returns an error, the iteration stops.
func (t *Table) Iterate(fn func(r document.Record) error) error {
	// To avoid unnecessary allocations, we create the slice once and reuse it
	// at each call of the fn method.
	// Since the AscendGreaterOrEqual is never supposed to call the callback concurrently
	// we can assume that it's thread safe.
	// TODO(asdine) Add a mutex if proven necessary
	var r encodedRecordWithKey

	return t.Store.AscendGreaterOrEqual(nil, func(k, v []byte) error {
		r.EncodedRecord = v
		r.key = k
		// r must be passed as pointer, not value, because passing a value to an interface
		// requires an allocation, while it doesn't for a pointer.
		return fn(&r)
	})
}

// GetRecord returns one record by key.
func (t *Table) GetRecord(key []byte) (document.Record, error) {
	v, err := t.Store.Get(key)
	if err != nil {
		if err == engine.ErrKeyNotFound {
			return nil, ErrRecordNotFound
		}
		return nil, errors.Wrapf(err, "failed to fetch record %q", key)
	}

	var r encodedRecordWithKey
	r.EncodedRecord = document.EncodedRecord(v)
	r.key = key
	return &r, err
}

func (t *Table) generateKey(r document.Record) ([]byte, error) {
	cfg, err := t.CfgStore.Get(t.name)
	if err != nil {
		return nil, err
	}

	var key []byte
	if cfg.PrimaryKeyName != "" {
		f, err := r.GetField(cfg.PrimaryKeyName)
		if err != nil {
			return nil, err
		}
		v, err := f.ConvertTo(cfg.PrimaryKeyType)
		if err != nil {
			return nil, err
		}
		return v.Data, nil
	}

	t.tx.db.mu.Lock()
	defer t.tx.db.mu.Unlock()

	cfg, err = t.CfgStore.Get(t.name)
	if err != nil {
		return nil, err
	}

	cfg.lastKey++
	key = value.NewInt64(cfg.lastKey).Data
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
func (t *Table) Insert(r document.Record) ([]byte, error) {
	key, err := t.generateKey(r)
	if err != nil {
		return nil, err
	}

	_, err = t.Store.Get(key)
	if err == nil {
		return nil, ErrDuplicateRecord
	}

	v, err := document.Encode(r)
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
		f, err := r.GetField(idx.FieldName)
		if err != nil {
			f.Value = value.NewNull()
		}

		err = idx.Set(f.Value, key)
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
		f, err := r.GetField(idx.FieldName)
		if err != nil {
			return err
		}

		err = idx.Delete(f.Value, key)
		if err != nil {
			return err
		}
	}

	return t.Store.Delete(key)
}

// Replace a record by key.
// An error is returned if the key doesn't exist.
// Indexes are automatically updated.
func (t *Table) Replace(key []byte, r document.Record) error {
	indexes, err := t.Indexes()
	if err != nil {
		return err
	}

	return t.replace(indexes, key, r)
}

func (t *Table) replace(indexes map[string]Index, key []byte, r document.Record) error {
	// make sure key exists
	old, err := t.GetRecord(key)
	if err != nil {
		return err
	}

	// remove key from indexes
	for _, idx := range indexes {
		f, err := old.GetField(idx.FieldName)
		if err != nil {
			return err
		}

		err = idx.Delete(f.Value, key)
		if err != nil {
			return err
		}
	}

	// encode new record
	v, err := document.Encode(r)
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
		f, err := r.GetField(idx.FieldName)
		if err != nil {
			continue
		}

		err = idx.Set(f.Value, key)
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
		Filter(func(r document.Record) (bool, error) {
			f, err := r.GetField("TableName")
			if err != nil {
				return false, err
			}

			return bytes.Equal(f.Data, tableName), nil
		}).
		Iterate(func(r document.Record) error {
			var opts indexOptions
			err := opts.ScanRecord(r)
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
