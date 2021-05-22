package database

import (
	"bytes"
	"encoding/binary"
	"errors"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/document/encoding"
	"github.com/genjidb/genji/engine"
	errs "github.com/genjidb/genji/errors"
	"github.com/genjidb/genji/internal/stringutil"
)

// A Table represents a collection of documents.
type Table struct {
	Tx    *Transaction
	Store engine.Store
	Name  string
	// Table information.
	// May not represent the most up to date data.
	// Always get a fresh Table instance before relying on this field.
	Info *TableInfo
	// List of Indexes of this table.
	// May not represent the most up to date data.
	// Always get a fresh Table instance relying on this field.
	Indexes Indexes
}

// Truncate deletes all the documents from the table.
func (t *Table) Truncate() error {
	return t.Store.Truncate()
}

// Insert the document into the table.
// If a primary key has been specified during the table creation, the field is expected to be present
// in the given document.
// If no primary key has been selected, a monotonic autoincremented integer key will be generated.
// It returns the inserted document alongside its key. They key can be accessed using the document.Keyer interface.
func (t *Table) Insert(d document.Document) (document.Document, error) {
	if t.Info.ReadOnly {
		return nil, errors.New("cannot write to read-only table")
	}

	fb, err := t.Info.FieldConstraints.ValidateDocument(d)
	if err != nil {
		return nil, err
	}

	key, err := t.generateKey(t.Info, fb)
	if err != nil {
		return nil, err
	}

	_, err = t.Store.Get(key)
	if err == nil {
		return nil, errs.ErrDuplicateDocument
	}

	var buf bytes.Buffer
	enc := t.Tx.DB.Codec.NewEncoder(&buf)
	defer enc.Close()
	err = enc.EncodeDocument(fb)
	if err != nil {
		return nil, stringutil.Errorf("failed to encode document: %w", err)
	}

	err = t.Store.Put(key, buf.Bytes())
	if err != nil {
		return nil, err
	}

	for _, idx := range t.Indexes {
		vs := make([]document.Value, 0, len(idx.Info.Paths))

		for _, path := range idx.Info.Paths {
			v, err := path.GetValueFromDocument(fb)
			if err != nil {
				v = document.NewNullValue()
			}

			vs = append(vs, v)
		}

		err = idx.Set(vs, key)
		if err != nil {
			if err == ErrIndexDuplicateValue {
				return nil, errs.ErrDuplicateDocument
			}

			return nil, err
		}
	}

	return documentWithKey{
		Document: d,
		key:      key,
		pk:       t.Info.GetPrimaryKey(),
	}, nil
}

// Delete a document by key.
// Indexes are automatically updated.
func (t *Table) Delete(key []byte) error {
	if t.Info.ReadOnly {
		return errors.New("cannot write to read-only table")
	}

	d, err := t.GetDocument(key)
	if err != nil {
		return err
	}

	for _, idx := range t.Indexes {
		vs := make([]document.Value, 0, len(idx.Info.Paths))
		for _, path := range idx.Info.Paths {
			v, err := path.GetValueFromDocument(d)
			if err != nil {
				if err == document.ErrFieldNotFound {
					v = document.NewNullValue()
				} else {
					return err
				}
			}

			vs = append(vs, v)
		}

		err = idx.Delete(vs, key)
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
	if t.Info.ReadOnly {
		return errors.New("cannot write to read-only table")
	}

	d, err := t.Info.FieldConstraints.ValidateDocument(d)
	if err != nil {
		return err
	}

	return t.replace(t.Indexes, key, d)
}

func (t *Table) replace(indexes []*Index, key []byte, d document.Document) error {
	// make sure key exists
	old, err := t.GetDocument(key)
	if err != nil {
		return err
	}

	// remove key from indexes
	for _, idx := range indexes {
		vs := make([]document.Value, 0, len(idx.Info.Paths))
		for _, path := range idx.Info.Paths {
			v, err := path.GetValueFromDocument(old)
			if err != nil {
				v = document.NewNullValue()
			}
			vs = append(vs, v)
		}

		err := idx.Delete(vs, key)
		if err != nil {
			return err
		}
	}

	// encode new document
	var buf bytes.Buffer
	enc := t.Tx.DB.Codec.NewEncoder(&buf)
	defer enc.Close()
	err = enc.EncodeDocument(d)
	if err != nil {
		return stringutil.Errorf("failed to encode document: %w", err)
	}

	// replace old document with new document
	err = t.Store.Put(key, buf.Bytes())
	if err != nil {
		return err
	}

	// update indexes
	for _, idx := range indexes {
		vs := make([]document.Value, 0, len(idx.Info.Paths))
		for _, path := range idx.Info.Paths {
			v, err := path.GetValueFromDocument(d)
			if err != nil {
				v = document.NewNullValue()
			}

			vs = append(vs, v)
		}

		err = idx.Set(vs, key)
		if err != nil {
			if err == ErrIndexDuplicateValue {
				return errs.ErrDuplicateDocument
			}

			return err
		}
	}

	return nil
}

type documentWithKey struct {
	document.Document

	key []byte
	pk  *FieldConstraint
}

func (e documentWithKey) MarshalJSON() ([]byte, error) {
	return document.MarshalJSON(e)
}

func (e documentWithKey) RawKey() []byte {
	return e.key
}

func (e documentWithKey) Key() (document.Value, error) {
	if e.pk == nil {
		docid, _ := binary.Uvarint(e.key)
		return document.NewIntegerValue(int64(docid)), nil
	}

	return e.pk.Path.GetValueFromDocument(&e)
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

	return d.pk.Path.GetValueFromDocument(d)
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

func (d *lazilyDecodedDocument) MarshalJSON() ([]byte, error) {
	return document.MarshalJSON(d)
}

// Iterate goes through all the documents of the table and calls the given function by passing each one of them.
// If the given function returns an error, the iteration stops.
func (t *Table) Iterate(fn func(d document.Document) error) error {
	return t.AscendGreaterOrEqual(document.Value{}, fn)
}

// EncodeValue encodes a value following primary key constraints.
// It returns a binary representation of the key as used in the store.
// It can be used to manually add a new entry to the store or to compare
// with other keys during table iteration.
func (t *Table) EncodeValue(v document.Value) ([]byte, error) {
	return t.encodeValueToKey(t.Info, v)
}

func (t *Table) encodeValueToKey(info *TableInfo, v document.Value) ([]byte, error) {
	var err error

	pk := t.Info.GetPrimaryKey()
	if pk == nil {
		// if no primary key was defined, convert the pivot to an integer then to an unsigned integer
		// and encode it as a varint
		v, err = v.CastAsInteger()
		if err != nil {
			return nil, err
		}

		docid := uint64(v.V.(int64))

		buf := make([]byte, binary.MaxVarintLen64)
		n := binary.PutUvarint(buf, docid)
		return buf[:n], nil
	}

	// if a primary key was defined and the primary is typed, convert the value to the right type.
	if !pk.Type.IsAny() {
		v, err = v.CastAs(pk.Type)
		if err != nil {
			return nil, err
		}

		return v.MarshalBinary()
	}

	// it no primary key type is specified,
	// and the value to encode is an integer
	// convert it to a double.
	if v.Type == document.IntegerValue {
		v, err = v.CastAsDouble()
		if err != nil {
			return nil, err
		}
	}

	// encode key regardless of type.
	var buf bytes.Buffer
	err = document.NewValueEncoder(&buf).Encode(v)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// AscendGreaterOrEqual iterates over the documents of the table whose key
// is greater than or equal to the pivot.
// The pivot is converted to the type of the primary key, if any, prior to iteration.
// If the pivot is empty, it iterates from the beginning of the table.
func (t *Table) AscendGreaterOrEqual(pivot document.Value, fn func(d document.Document) error) error {
	return t.iterate(pivot, false, fn)
}

// DescendLessOrEqual iterates over the documents of the table whose key
// is less than or equal to the pivot, in reverse order.
// The pivot is converted to the type of the primary key, if any, prior to iteration.
// If the pivot is empty, it iterates from the end of the table in reverse order.
func (t *Table) DescendLessOrEqual(pivot document.Value, fn func(d document.Document) error) error {
	return t.iterate(pivot, true, fn)
}

func (t *Table) iterate(pivot document.Value, reverse bool, fn func(d document.Document) error) error {
	var seek []byte

	// if there is a pivot, convert it to the right type
	if !pivot.Type.IsAny() && pivot.V != nil {
		var err error
		seek, err = t.encodeValueToKey(t.Info, pivot)
		if err != nil {
			return err
		}
	}

	// To avoid unnecessary allocations, we create the struct once and reuse
	// it during each iteration.
	d := lazilyDecodedDocument{
		codec: t.Tx.DB.Codec,
	}

	d.pk = t.Info.GetPrimaryKey()

	it := t.Store.Iterator(engine.IteratorOptions{Reverse: reverse})
	defer it.Close()

	for it.Seek(seek); it.Valid(); it.Next() {
		d.Reset()
		d.item = it.Item()
		// d must be passed as pointer, not value,
		// because passing a value to an interface
		// requires an allocation, while it doesn't for a pointer.
		err := fn(&d)
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
			return nil, errs.ErrDocumentNotFound
		}
		return nil, stringutil.Errorf("failed to fetch document %q: %w", key, err)
	}

	var d documentWithKey
	d.Document = t.Tx.DB.Codec.NewDocument(v)
	d.key = key
	d.pk = t.Info.GetPrimaryKey()
	return &d, err
}

// generate a key for d based on the table configuration.
// if the table has a primary key, it extracts the field from
// the document, converts it to the targeted type and returns
// its encoded version.
// if there are no primary key in the table, a default
// key is generated, called the docid.
func (t *Table) generateKey(info *TableInfo, fb *document.FieldBuffer) ([]byte, error) {
	if pk := t.Info.GetPrimaryKey(); pk != nil {

		v, err := pk.Path.GetValueFromDocument(fb)
		if err == document.ErrFieldNotFound {
			return nil, stringutil.Errorf("missing primary key at path %q", pk.Path)
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
