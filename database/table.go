package database

import (
	"bytes"
	"errors"
	"fmt"
	"math"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/document/encoding/msgpack"
	"github.com/genjidb/genji/engine"
	"github.com/genjidb/genji/index"
	"github.com/genjidb/genji/key"
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
	return t.infoStore.Get(t.tx, t.name)
}

// Name returns the name of the table.
func (t *Table) Name() string {
	return t.name
}

// Truncate deletes all the documents from the table.
func (t *Table) Truncate() error {
	return t.Store.Truncate()
}

// Insert the document into the table.
// If a primary key has been specified during the table creation, the field is expected to be present
// in the given document.
// If no primary key has been selected, a monotonic autoincremented integer key will be generated.
func (t *Table) Insert(d document.Document) ([]byte, error) {
	info, err := t.Info()
	if err != nil {
		return nil, err
	}

	if info.readOnly {
		return nil, errors.New("cannot write to read-only table")
	}

	d, err = t.ValidateConstraints(d)
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
		v, err := idx.Opts.Path.GetValue(d)
		if err != nil {
			v = document.NewNullValue()
		}

		// arrays and documents are not indexed.
		if v.Type == document.ArrayValue || v.Type == document.DocumentValue {
			continue
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
	info, err := t.Info()
	if err != nil {
		return err
	}

	if info.readOnly {
		return errors.New("cannot write to read-only table")
	}

	d, err := t.GetDocument(key)
	if err != nil {
		return err
	}

	indexes, err := t.Indexes()
	if err != nil {
		return err
	}

	for _, idx := range indexes {
		v, err := idx.Opts.Path.GetValue(d)
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
	info, err := t.Info()
	if err != nil {
		return err
	}

	if info.readOnly {
		return errors.New("cannot write to read-only table")
	}

	d, err = t.ValidateConstraints(d)
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
		v, err := idx.Opts.Path.GetValue(old)
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
		v, err := idx.Opts.Path.GetValue(d)
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

	indexes := make(map[string]Index)

	err = document.NewStream(&tb).
		Filter(func(d document.Document) (bool, error) {
			v, err := d.GetByField("tablename")
			if err != nil {
				return false, err
			}

			return v.V.(string) == t.name, nil
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
				Index: idx,
				Opts:  opts,
			}

			return nil
		})
	if err != nil {
		return nil, err
	}

	return indexes, nil
}

type encodedDocumentWithKey struct {
	msgpack.EncodedDocument

	key []byte
}

func (e encodedDocumentWithKey) Key() []byte {
	return e.key
}

// This document implementation waits until
// GetByField or Iterate are called to
// fetch the value from the engine store.
// This is useful to prevent reading the value
// from store on documents that don't need to be
// decoded.
type lazilyDecodedDocument struct {
	item engine.Item
	buf  msgpack.EncodedDocument
}

func (d *lazilyDecodedDocument) GetByField(field string) (v document.Value, err error) {
	if len(d.buf) == 0 {
		d.copyFromItem()
	}

	return d.buf.GetByField(field)
}

func (d *lazilyDecodedDocument) Iterate(fn func(field string, value document.Value) error) error {
	if len(d.buf) == 0 {
		d.copyFromItem()
	}

	return d.buf.Iterate(fn)
}

func (d *lazilyDecodedDocument) Key() []byte {
	return d.item.Key()
}

func (d *lazilyDecodedDocument) Reset() {
	d.buf = d.buf[:0]
	d.item = nil
}

func (d *lazilyDecodedDocument) copyFromItem() error {
	var err error
	d.buf, err = d.item.ValueCopy(d.buf[:0])

	return err
}

// Iterate goes through all the documents of the table and calls the given function by passing each one of them.
// If the given function returns an error, the iteration stops.
func (t *Table) Iterate(fn func(d document.Document) error) error {
	// To avoid unnecessary allocations, we create the struct once and reuse
	// it during each iteration.
	var d lazilyDecodedDocument

	it := t.Store.NewIterator(engine.IteratorConfig{})
	defer it.Close()

	var err error
	for it.Seek(nil); it.Valid(); it.Next() {
		d.Reset()
		d.item = it.Item()
		// d must be passed as pointer, not value,
		// because passing a value to an interface
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
	ti, err := t.infoStore.Get(t.tx, t.name)
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

		return key.AppendValue(nil, v), nil
	}

	docid, err := t.generateDocid()
	if err != nil {
		return nil, err
	}

	return key.AppendInt64(nil, docid), nil
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
			t.tx.db.tableDocids[t.name], err = key.DecodeInt64(it.Item().Key())
			if err != nil {
				it.Close()
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

	var buf [8]byte
	for it.Seek(nil); it.Valid(); it.Next() {
		if !bytes.Equal(it.Item().Key(), key.AppendInt64(buf[:0], i)) {
			return i, nil
		}
		i++
	}

	return 0, errors.New("reached maximum number of documents in a table")
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
		if field.FieldName == "" {
			// if the field is not found we make sure it is not required
			if c.IsNotNull {
				return fmt.Errorf("field %q is required and must be not null", c.Path)
			}
			return nil
		}

		v, err := buf.GetByField(field.FieldName)
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

		v, err = v.CastAs(c.Type)
		if err != nil {
			return err
		}

		err = buf.Replace(field.FieldName, v)
		if err != nil {
			return err
		}
	case document.ArrayValue:
		// if it's an array, we can assume it's a ValueBuffer
		buf := parent.V.(*document.ValueBuffer)

		frag := c.Path[len(c.Path)-1]
		if frag.FieldName != "" {
			// if the fieldName is not empty, then the path must refer to a document and not an array,
			// we simply skip
			return nil
		}

		v, err := buf.GetByIndex(frag.ArrayIndex)
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

		v, err = v.CastAs(c.Type)
		if err != nil {
			return err
		}

		err = buf.Replace(frag.ArrayIndex, v)
		if err != nil {
			return err
		}
	}

	return nil
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

// ReIndex all the indexes of the table.
func (t *Table) ReIndex() error {
	info, err := t.Info()
	if err != nil {
		return err
	}

	if info.readOnly {
		return errors.New("cannot write to read-only table")
	}

	indexes, err := t.Indexes()
	if err != nil {
		return err
	}

	for _, idx := range indexes {
		err = t.tx.ReIndex(idx.Opts.IndexName)
		if err != nil {
			return err
		}
	}

	return nil
}
