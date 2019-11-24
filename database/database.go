package database

import (
	"bytes"
	"sync"

	"github.com/asdine/genji/engine"
	"github.com/asdine/genji/index"
	"github.com/asdine/genji/record"
	"github.com/asdine/genji/value"
	"github.com/pkg/errors"
)

var (
	separator            byte = 0x1F
	tableConfigStoreName      = "__genji.tables"
	indexStoreName            = "__genji.indexes"
	indexPrefix               = "i"
)

type Database struct {
	ng engine.Engine

	mu sync.Mutex
}

// New initializes the DB using the given engine.
func New(ng engine.Engine) (*Database, error) {
	db := Database{
		ng: ng,
	}

	ntx, err := db.ng.Begin(true)
	if err != nil {
		return nil, err
	}
	defer ntx.Rollback()

	_, err = ntx.Store(tableConfigStoreName)
	if err == engine.ErrStoreNotFound {
		err = ntx.CreateStore(tableConfigStoreName)
	}
	if err != nil {
		return nil, err
	}

	_, err = ntx.Store(indexStoreName)
	if err == engine.ErrStoreNotFound {
		err = ntx.CreateStore(indexStoreName)
	}
	if err != nil {
		return nil, err
	}

	err = ntx.Commit()
	if err != nil {
		return nil, err
	}

	return &db, nil
}

// Close the underlying engine.
func (db *Database) Close() error {
	return db.ng.Close()
}

// Begin starts a new transaction.
// The returned transaction must be closed either by calling Rollback or Commit.
func (db *Database) Begin(writable bool) (*Transaction, error) {
	ntx, err := db.ng.Begin(writable)
	if err != nil {
		return nil, err
	}

	tx := Transaction{
		db:       db,
		tx:       ntx,
		writable: writable,
	}

	tx.tcfgStore, err = tx.getTableConfigStore()
	if err != nil {
		return nil, err
	}

	tx.indexStore, err = tx.getIndexStore()
	if err != nil {
		return nil, err
	}

	return &tx, nil
}

// A Table represents a collection of records.
type Table struct {
	tx       *Transaction
	store    engine.Store
	name     string
	cfgStore *tableConfigStore
}

type encodedRecordWithKey struct {
	record.EncodedRecord

	key []byte
}

func (e encodedRecordWithKey) Key() []byte {
	return e.key
}

// Iterate goes through all the records of the table and calls the given function by passing each one of them.
// If the given function returns an error, the iteration stops.
func (t *Table) Iterate(fn func(r record.Record) error) error {
	// To avoid unnecessary allocations, we create the slice once and reuse it
	// at each call of the fn method.
	// Since the AscendGreaterOrEqual is never supposed to call the callback concurrently
	// we can assume that it's thread safe.
	// TODO(asdine) Add a mutex if proven necessary
	var r encodedRecordWithKey

	return t.store.AscendGreaterOrEqual(nil, func(k, v []byte) error {
		r.EncodedRecord = v
		r.key = k
		// r must be passed as pointer, not value, because passing a value to an interface
		// requires an allocation, while it doesn't for a pointer.
		return fn(&r)
	})
}

// GetRecord returns one record by key.
func (t *Table) GetRecord(key []byte) (record.Record, error) {
	v, err := t.store.Get(key)
	if err != nil {
		if err == engine.ErrKeyNotFound {
			return nil, ErrRecordNotFound
		}
		return nil, errors.Wrapf(err, "failed to fetch record %q", key)
	}

	var r encodedRecordWithKey
	r.EncodedRecord = record.EncodedRecord(v)
	r.key = key
	return &r, err
}

func (t *Table) generateKey(r record.Record) ([]byte, error) {
	cfg, err := t.cfgStore.Get(t.name)
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

	cfg, err = t.cfgStore.Get(t.name)
	if err != nil {
		return nil, err
	}

	cfg.lastKey++
	key = value.NewInt64(cfg.lastKey).Data
	err = t.cfgStore.Replace(t.name, cfg)
	if err != nil {
		return nil, err
	}

	return key, nil
}

// Insert the record into the table.
// If a primary key has been specified during the table creation, the field is expected to be present
// in the given record.
// If no primary key has been selected, a monotonic autoincremented integer key will be generated.
func (t *Table) Insert(r record.Record) ([]byte, error) {
	key, err := t.generateKey(r)
	if err != nil {
		return nil, err
	}

	_, err = t.store.Get(key)
	if err == nil {
		return nil, ErrDuplicateRecord
	}

	v, err := record.Encode(r)
	if err != nil {
		return nil, errors.Wrap(err, "failed to encode record")
	}

	err = t.store.Put(key, v)
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

	return t.store.Delete(key)
}

// Replace a record by key.
// An error is returned if the key doesn't exist.
// Indexes are automatically updated.
func (t *Table) Replace(key []byte, r record.Record) error {
	indexes, err := t.Indexes()
	if err != nil {
		return err
	}

	return t.replace(indexes, key, r)
}

func (t *Table) replace(indexes map[string]Index, key []byte, r record.Record) error {
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
	v, err := record.Encode(r)
	if err != nil {
		return errors.Wrap(err, "failed to encode record")
	}

	// replace old record with new record
	err = t.store.Put(key, v)
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
	return t.store.Truncate()
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
		store: s,
		name:  indexStoreName,
	}

	tableName := []byte(t.name)
	indexes := make(map[string]Index)

	err = record.NewStream(&tb).
		Filter(func(r record.Record) (bool, error) {
			f, err := r.GetField("TableName")
			if err != nil {
				return false, err
			}

			return bytes.Equal(f.Data, tableName), nil
		}).
		Iterate(func(r record.Record) error {
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

type indexOptions struct {
	IndexName string
	TableName string
	FieldName string
	Unique    bool
}

// Field implements the field method of the record.Record interface.
func (i *indexOptions) GetField(name string) (record.Field, error) {
	switch name {
	case "IndexName":
		return record.NewStringField("IndexName", i.IndexName), nil
	case "TableName":
		return record.NewStringField("TableName", i.TableName), nil
	case "FieldName":
		return record.NewStringField("FieldName", i.FieldName), nil
	case "Unique":
		return record.NewBoolField("Unique", i.Unique), nil
	}

	return record.Field{}, errors.New("unknown field")
}

// Iterate through all the fields one by one and pass each of them to the given function.
// It the given function returns an error, the iteration is interrupted.
func (i *indexOptions) Iterate(fn func(record.Field) error) error {
	var err error
	var f record.Field

	f, _ = i.GetField("IndexName")
	err = fn(f)
	if err != nil {
		return err
	}

	f, _ = i.GetField("TableName")
	err = fn(f)
	if err != nil {
		return err
	}

	f, _ = i.GetField("FieldName")
	err = fn(f)
	if err != nil {
		return err
	}

	f, _ = i.GetField("Unique")
	err = fn(f)
	if err != nil {
		return err
	}

	return nil
}

// ScanRecord extracts fields from record and assigns them to the struct fields.
// It implements the record.Scanner interface.
func (i *indexOptions) ScanRecord(rec record.Record) error {
	return rec.Iterate(func(f record.Field) error {
		var err error

		switch f.Name {
		case "IndexName":
			i.IndexName, err = value.DecodeString(f.Data)
		case "TableName":
			i.TableName, err = value.DecodeString(f.Data)
		case "FieldName":
			i.FieldName, err = value.DecodeString(f.Data)
		case "Unique":
			i.Unique, err = value.DecodeBool(f.Data)
		}
		return err
	})
}
