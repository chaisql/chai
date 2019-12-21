package database

import (
	"bytes"

	"github.com/asdine/genji/document"
	"github.com/asdine/genji/document/encoding"
	"github.com/asdine/genji/engine"
	"github.com/asdine/genji/index"
	"github.com/pkg/errors"
)

// A Table represents a collection of documents.
type Table struct {
	tx       *Transaction
	Store    engine.Store
	name     string
	cfgStore *tableConfigStore
}

// Config of the table.
func (t *Table) Config() (*TableConfig, error) {
	return t.cfgStore.Get(t.name)
}

type encodedDocumentWithKey struct {
	encoding.EncodedDocument

	key []byte
}

func (e encodedDocumentWithKey) Key() []byte {
	return e.key
}

// Iterate goes through all the documents of the table and calls the given function by passing each one of them.
// If the given function returns an error, the iteration stops.
func (t *Table) Iterate(fn func(d document.Document) error) error {
	// To avoid unnecessary allocations, we create the slice once and reuse it
	// at each call of the fn method.
	// Since the AscendGreaterOrEqual is never supposed to call the callback concurrently
	// we can assume that it's thread safe.
	// TODO(asdine) Add a mutex if proven necessary
	var d encodedDocumentWithKey

	return t.Store.AscendGreaterOrEqual(nil, func(k, v []byte) error {
		d.EncodedDocument = v
		d.key = k
		// r must be passed as pointer, not value, because passing a value to an interface
		// requires an allocation, while it doesn't for a pointer.
		return fn(&d)
	})
}

// GetDocument returns one document by key.
func (t *Table) GetDocument(key []byte) (document.Document, error) {
	v, err := t.Store.Get(key)
	if err != nil {
		if err == engine.ErrKeyNotFound {
			return nil, ErrDocumentNotFound
		}
		return nil, errors.Wrapf(err, "failed to fetch document %q", key)
	}

	var d encodedDocumentWithKey
	d.EncodedDocument = encoding.EncodedDocument(v)
	d.key = key
	return &d, err
}

func (t *Table) generateKey(d document.Document) ([]byte, error) {
	cfg, err := t.cfgStore.Get(t.name)
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

	cfg, err = t.cfgStore.Get(t.name)
	if err != nil {
		return nil, err
	}

	cfg.LastKey++
	key = encoding.EncodeInt64(cfg.LastKey)
	err = t.cfgStore.Replace(t.name, cfg)
	if err != nil {
		return nil, err
	}

	return key, nil
}

func (t *Table) validateConstraints(d document.Document) (document.Document, error) {
	cfg, err := t.Config()
	if err != nil {
		return nil, err
	}

	if len(cfg.FieldConstraints) == 0 {
		return d, nil
	}

	var fb document.FieldBuffer

	err = d.Iterate(func(field string, v document.Value) error {
		for _, fc := range cfg.FieldConstraints {
			if fc.Name == field {
				v, err = v.ConvertTo(fc.Type)
				if err != nil {
					return err
				}
				break
			}
		}
		fb.Add(field, v)
		return nil
	})

	return &fb, err
}

// Insert the document into the table.
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
		return nil, ErrDuplicateDocument
	}

	d, err = t.validateConstraints(d)
	if err != nil {
		return nil, err
	}

	v, err := encoding.EncodeDocument(d)
	if err != nil {
		return nil, errors.Wrap(err, "failed to encode document")
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
		v, err := idx.Path.GetValue(d)
		if err != nil {
			v = document.NewNullValue()
		}

		err = idx.Set(v, key)
		if err != nil {
			if err == index.ErrDuplicate {
				return nil, ErrDuplicateDocument
			}

			return nil, err
		}
	}

	return key, nil
}

// Delete a document by key.
// Indexes are automatically updated.
func (t *Table) Delete(key []byte) error {
	r, err := t.GetDocument(key)
	if err != nil {
		return err
	}

	indexes, err := t.Indexes()
	if err != nil {
		return err
	}

	for _, idx := range indexes {
		v, err := idx.Path.GetValue(r)
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

// Replace a document by key.
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
	old, err := t.GetDocument(key)
	if err != nil {
		return err
	}

	// remove key from indexes
	for _, idx := range indexes {
		v, err := idx.Path.GetValue(old)
		if err != nil {
			return err
		}

		err = idx.Delete(v, key)
		if err != nil {
			return err
		}
	}

	// encode new document
	v, err := encoding.EncodeDocument(d)
	if err != nil {
		return errors.Wrap(err, "failed to encode document")
	}

	// replace old document with new document
	err = t.Store.Put(key, v)
	if err != nil {
		return err
	}

	// update indexes
	for _, idx := range indexes {
		v, err := idx.Path.GetValue(d)
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

// Truncate deletes all the documents from the table.
func (t *Table) Truncate() error {
	return t.Store.Truncate()
}

// TableName returns the name of the table.
func (t *Table) TableName() string {
	return t.name
}

// Indexes returns a map of all the indexes of a table.
func (t *Table) Indexes() (map[string]Index, error) {
	s, err := t.tx.Tx.Store(indexStoreName)
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
			v, err := d.GetByField("tablename")
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
			var opts IndexOptions
			err := document.StructScan(d, &opts)
			if err != nil {
				return err
			}

			var idx index.Index
			if opts.Unique {
				idx = index.NewUniqueIndex(t.tx.Tx, opts.IndexName)
			} else {
				idx = index.NewListIndex(t.tx.Tx, opts.IndexName)
			}

			indexes[opts.Path.String()] = Index{
				Index:     idx,
				IndexName: opts.IndexName,
				TableName: opts.TableName,
				Path:      opts.Path,
				Unique:    opts.Unique,
			}

			return nil
		})
	if err != nil {
		return nil, err
	}

	return indexes, nil
}
