package database

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/document/encoding"
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

// Tx returns the current transaction.
func (t *Table) Tx() *Transaction {
	return t.tx
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

	fb, err := info.FieldConstraints.ValidateDocument(d)
	if err != nil {
		return nil, err
	}

	key, err := t.generateKey(info, fb)
	if err != nil {
		return nil, err
	}

	_, err = t.Store.Get(key)
	if err == nil {
		return nil, ErrDuplicateDocument
	}

	var buf bytes.Buffer
	enc := t.tx.db.Codec.NewEncoder(&buf)
	defer enc.Close()
	err = enc.EncodeDocument(fb)
	if err != nil {
		return nil, fmt.Errorf("failed to encode document: %w", err)
	}

	err = t.Store.Put(key, buf.Bytes())
	if err != nil {
		return nil, err
	}

	indexes, err := t.Indexes()
	if err != nil {
		return nil, err
	}

	for _, idx := range indexes {
		v, err := idx.Opts.Path.GetValue(fb)
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

	d, err = info.FieldConstraints.ValidateDocument(d)
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
	var buf bytes.Buffer
	enc := t.tx.db.Codec.NewEncoder(&buf)
	defer enc.Close()
	err = enc.EncodeDocument(d)
	if err != nil {
		return fmt.Errorf("failed to encode document: %w", err)
	}

	// replace old document with new document
	err = t.Store.Put(key, buf.Bytes())
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
	s, err := t.tx.tx.GetStore([]byte(indexStoreName))
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
			v, err := d.GetByField("table_name")
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

			idx := index.New(t.tx.tx, opts.IndexName, index.Options{
				Unique: opts.Unique,
				Type:   opts.Type,
			})

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
	document.Document

	key []byte
	pk  *FieldConstraint
}

func (e encodedDocumentWithKey) RawKey() []byte {
	return e.key
}

func (e encodedDocumentWithKey) Key() (document.Value, error) {
	if e.pk == nil {
		docid, _ := binary.Uvarint(e.key)
		return document.NewIntegerValue(int64(docid)), nil
	}

	return e.pk.Path.GetValue(&e)
}

// This document implementation waits until
// GetByField or Iterate are called to
// fetch the value from the engine store.
// This is useful to prevent reading the value
// from store on documents that don't need to be
// decoded.
type lazilyDecodedDocument struct {
	item  engine.Item
	buf   []byte
	codec encoding.Codec
	pk    *FieldConstraint
}

func (d *lazilyDecodedDocument) GetByField(field string) (v document.Value, err error) {
	if len(d.buf) == 0 {
		err = d.copyFromItem()
		if err != nil {
			return
		}
	}

	return d.codec.NewDocument(d.buf).GetByField(field)
}

func (d *lazilyDecodedDocument) Iterate(fn func(field string, value document.Value) error) error {
	if len(d.buf) == 0 {
		err := d.copyFromItem()
		if err != nil {
			return err
		}
	}

	return d.codec.NewDocument(d.buf).Iterate(fn)
}

func (d *lazilyDecodedDocument) RawKey() []byte {
	return d.item.Key()
}

func (d *lazilyDecodedDocument) Key() (document.Value, error) {
	k := d.item.Key()
	if d.pk == nil {
		docid, _ := binary.Uvarint(k)
		return document.NewIntegerValue(int64(docid)), nil
	}

	return d.pk.Path.GetValue(d)
}

func (d *lazilyDecodedDocument) Reset() {
	d.buf = d.buf[:0]
	d.item = nil
}

func (d *lazilyDecodedDocument) copyFromItem() error {
	var err error
	d.buf, err = d.item.ValueCopy(d.buf)

	return err
}

// Iterate goes through all the documents of the table and calls the given function by passing each one of them.
// If the given function returns an error, the iteration stops.
func (t *Table) Iterate(fn func(d document.Document) error) error {
	// To avoid unnecessary allocations, we create the struct once and reuse
	// it during each iteration.
	d := lazilyDecodedDocument{
		codec: t.tx.db.Codec,
	}

	info, err := t.Info()
	if err != nil {
		return err
	}
	d.pk = info.GetPrimaryKey()

	it := t.Store.Iterator(engine.IteratorOptions{})
	defer it.Close()

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
	if err := it.Err(); err != nil {
		return err
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

	info, err := t.Info()
	if err != nil {
		return nil, err
	}

	var d encodedDocumentWithKey
	d.Document = t.tx.db.Codec.NewDocument(v)
	d.key = key
	d.pk = info.GetPrimaryKey()
	return &d, err
}

// generate a key for d based on the table configuration.
// if the table has a primary key, it extracts the field from
// the document, converts it to the targeted type and returns
// its encoded version.
// if there are no primary key in the table, a default
// key is generated, called the docid.
func (t *Table) generateKey(info *TableInfo, fb *document.FieldBuffer) ([]byte, error) {
	if pk := info.GetPrimaryKey(); pk != nil {

		v, err := pk.Path.GetValue(fb)
		if err == document.ErrFieldNotFound {
			return nil, fmt.Errorf("missing primary key at path %q", pk.Path)
		}
		if err != nil {
			return nil, err
		}

		// if a primary key type is specified,
		// encode the key using the optimized encoding solution
		if pk.Type != 0 {
			return v.MarshalBinary()
		}

		// it no primary key type is specified,
		// encode keys regardless of type.
		var buf bytes.Buffer
		err = document.NewValueEncoder(&buf).Encode(v)
		if err != nil {
			return nil, err
		}
		return buf.Bytes(), nil
	}

	docid, err := t.Store.NextSequence()
	if err != nil {
		return nil, err
	}

	buf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(buf, docid)
	return buf[:n], nil
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
