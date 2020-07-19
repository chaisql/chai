package database

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"strconv"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/document/encoding"
	"github.com/genjidb/genji/document/encoding/msgpack"
	"github.com/genjidb/genji/engine"
	"github.com/genjidb/genji/index"
)

// A Table represents a collection of documents.
type Table struct {
	tx        *Transaction
	Store     engine.Store
	name      string
	infoStore *tableInfoStore
}

// Info of the table.
func (t *Table) Info() (*TableInfo, error) {
	ti, err := t.infoStore.Get(t.name)
	if err != nil {
		return nil, err
	}

	return ti, nil
}

type encodedDocumentWithKey struct {
	msgpack.EncodedDocument

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

	it := t.Store.NewIterator(engine.IteratorConfig{})
	defer it.Close()

	var err error
	for it.Seek(nil); it.Valid(); it.Next() {
		item := it.Item()
		d.EncodedDocument, err = item.ValueCopy(d.EncodedDocument[:0])
		if err != nil {
			return err
		}

		d.key = item.Key()
		// r must be passed as pointer, not value, because passing a value to an interface
		// requires an allocation, while it doesn't for a pointer.
		err = fn(&d)
		if err != nil {
			return err
		}
	}

	return nil
}

// GetDocument returns one document by key.
func (t *Table) GetDocument(key []byte) (document.Document, error) {
	v, err := t.Store.Get(key)
	if err != nil {
		if err == engine.ErrKeyNotFound {
			return nil, ErrDocumentNotFound
		}
		return nil, fmt.Errorf("failed to fetch document %q: %w", key, err)
	}

	var d encodedDocumentWithKey
	d.EncodedDocument = msgpack.EncodedDocument(v)
	d.key = key
	return &d, err
}

// generate a key for d based on the table configuration.
// if the table has a primary key, it extracts the field from
// the document, converts it to the targeted type and returns
// its encoded version.
// if there are no primary key in the table, a default
// key is generated, called the docid.
func (t *Table) generateKey(d document.Document) ([]byte, error) {
	ti, err := t.infoStore.Get(t.name)
	if err != nil {
		return nil, err
	}

	if pk := ti.GetPrimaryKey(); pk != nil {
		v, err := pk.Path.GetValue(d)
		if err == document.ErrFieldNotFound {
			return nil, fmt.Errorf("missing primary key at path %q", pk.Path)
		}
		if err != nil {
			return nil, err
		}

		return encoding.EncodeValue(v)
	}

	docid, err := t.generateDocid()
	if err != nil {
		return nil, err
	}

	return encoding.EncodeInt64(docid), nil
}

// this function looks up for the highest key in the table,
// increments it, caches it in the database tableDocids map
// and returns it.
// if the docid is greater than max.Int64, it looks up for the lowest
// available docid in the table.
// Generating a docid is safe for concurrent access across
// multiple transactions.
func (t *Table) generateDocid() (int64, error) {
	t.tx.db.mu.Lock()
	defer t.tx.db.mu.Unlock()

	var err error

	// get the cached latest docid
	lastDocid, ok := t.tx.db.tableDocids[t.name]
	// if no key was found in the cache, get the largest key in the table
	if !ok {
		it := t.Store.NewIterator(engine.IteratorConfig{Reverse: true})
		it.Seek(nil)
		if it.Valid() {
			t.tx.db.tableDocids[t.name], err = encoding.DecodeInt64(it.Item().Key())
			if err != nil {
				return 0, err
			}
		} else {
			t.tx.db.tableDocids[t.name] = 0
		}

		err = it.Close()
		if err != nil {
			return 0, err
		}

		lastDocid = t.tx.db.tableDocids[t.name]
	}

	// if the id is bigger than an int64
	// look for the smallest available docid
	if lastDocid > math.MaxInt64-1 {
		return t.getSmallestAvailableDocid()
	}

	lastDocid++

	// cache it
	t.tx.db.tableDocids[t.name] = lastDocid
	return lastDocid, nil
}

// getSmallestAvailableDocid iterates through the table store
// and returns the first available docid.
func (t *Table) getSmallestAvailableDocid() (int64, error) {
	it := t.Store.NewIterator(engine.IteratorConfig{})
	defer it.Close()

	var i int64 = 1

	for it.Seek(nil); it.Valid(); it.Next() {
		if !bytes.Equal(it.Item().Key(), encoding.EncodeInt64(i)) {
			return i, nil
		}
		i++
	}

	return 0, errors.New("reached maximum number of documents in a table")
}

func getParentValue(d document.Document, p document.ValuePath) (document.Value, error) {
	if len(p) == 0 {
		return document.Value{}, errors.New("empty path")
	}

	if len(p) == 1 {
		return document.NewDocumentValue(d), nil
	}

	return p[:len(p)-1].GetValue(d)
}

// ValidateConstraints check the table configuration for constraints and validates the document
// against them. If the types defined by the constraints are different than the ones found in
// the document, the fields are converted to these types when possible. if the conversion
// fails, an error is returned.
func (t *Table) ValidateConstraints(d document.Document) (document.Document, error) {
	info, err := t.Info()
	if err != nil {
		return nil, err
	}

	pk := info.GetPrimaryKey()

	if len(info.FieldConstraints) == 0 && pk == nil {
		return d, nil
	}

	var fb document.FieldBuffer

	// make sure the document tree is full of document.FieldBuffer or document.ValueBuffer
	// so we can modify them and convert field types.
	err = fb.Copy(d)
	if err != nil {
		return nil, err
	}

	if pk != nil {
		err = validateConstraint(&fb, pk)
		if err != nil {
			return nil, err
		}
	}

	for _, fc := range info.FieldConstraints {
		err := validateConstraint(&fb, &fc)
		if err != nil {
			return nil, err
		}
	}

	return &fb, err
}

func validateConstraint(d document.Document, c *FieldConstraint) error {
	// get the parent buffer
	parent, err := getParentValue(d, c.Path)
	if err != nil {
		return err
	}

	switch parent.Type {
	case document.DocumentValue:
		// if it's a document, we can assume it's a FieldBuffer
		buf := parent.V.(*document.FieldBuffer)

		// the field to modify is the last chunk of the path
		field := c.Path[len(c.Path)-1]

		v, err := buf.GetByField(field)
		// if the field is not found we make sure it is not required
		if err != nil {
			if err == document.ErrFieldNotFound {
				if c.IsNotNull {
					return fmt.Errorf("field %q is required and must be not null", c.Path)
				}

				return nil
			}

			return err
		}
		// if the field is null we make sure it is not required
		if v.Type == document.NullValue && c.IsNotNull {
			return fmt.Errorf("field %q is required and must be not null", c.Path)
		}

		// if not we convert it and replace it in the buffer

		// if no type was provided, no need to convert though
		if c.Type == 0 {
			return nil
		}

		v, err = v.ConvertTo(c.Type)
		if err != nil {
			return err
		}

		err = buf.Replace(field, v)
		if err != nil {
			return err
		}
	case document.ArrayValue:
		// if it's an array, we can assume it's a ValueBuffer
		buf := parent.V.(*document.ValueBuffer)

		// the index to modify if the last chunk of the path
		index, err := strconv.Atoi(c.Path[len(c.Path)-1])
		// if there is an error, then the path must refer to a document and not an array,
		// we simply skip
		if err != nil {
			return nil
		}

		v, err := buf.GetByIndex(index)
		// if the value is not found we make sure it is not required,
		if err != nil {
			if err == document.ErrValueNotFound {
				if c.IsNotNull {
					return fmt.Errorf("value %q is required and must be not null", c.Path)
				}

				return nil
			}

			return err
		}

		// if not we convert it and replace it in the buffer
		if c.Type == 0 {
			return nil
		}

		v, err = v.ConvertTo(c.Type)
		if err != nil {
			return err
		}

		err = buf.Replace(index, v)
		if err != nil {
			return err
		}
	}

	return nil
}

// Insert the document into the table.
// If a primary key has been specified during the table creation, the field is expected to be present
// in the given document.
// If no primary key has been selected, a monotonic autoincremented integer key will be generated.
func (t *Table) Insert(d document.Document) ([]byte, error) {
	d, err := t.ValidateConstraints(d)
	if err != nil {
		return nil, err
	}

	key, err := t.generateKey(d)
	if err != nil {
		return nil, err
	}

	_, err = t.Store.Get(key)
	if err == nil {
		return nil, ErrDuplicateDocument
	}

	v, err := msgpack.EncodeDocument(d)
	if err != nil {
		return nil, fmt.Errorf("failed to encode document: %w", err)
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
	d, err := t.GetDocument(key)
	if err != nil {
		return err
	}

	indexes, err := t.Indexes()
	if err != nil {
		return err
	}

	for _, idx := range indexes {
		v, err := idx.Path.GetValue(d)
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
	d, err := t.ValidateConstraints(d)
	if err != nil {
		return err
	}

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
	v, err := msgpack.EncodeDocument(d)
	if err != nil {
		return fmt.Errorf("failed to encode document: %w", err)
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

// Name returns the name of the table.
func (t *Table) Name() string {
	return t.name
}

// Indexes returns a map of all the indexes of a table.
func (t *Table) Indexes() (map[string]Index, error) {
	s, err := t.tx.Tx.GetStore([]byte(indexStoreName))
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
			var opts IndexConfig
			err := opts.ScanDocument(d)
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
