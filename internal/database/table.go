package database

import (
	"bytes"
	"encoding/binary"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/document/encoding"
	"github.com/genjidb/genji/engine"
	errs "github.com/genjidb/genji/errors"
	"github.com/genjidb/genji/internal/errors"
	"github.com/genjidb/genji/internal/stringutil"
	"github.com/genjidb/genji/types"
	tencoding "github.com/genjidb/genji/types/encoding"
)

// A Table represents a collection of documents.
type Table struct {
	Tx    *Transaction
	Store engine.Store
	// Table information.
	// May not represent the most up to date data.
	// Always get a fresh Table instance before relying on this field.
	Info *TableInfo

	// List of Indexes of this table.
	// May not represent the most up to date data.
	// Always get a fresh Table instance before relying on this field.
	Indexes []*Index

	Catalog *Catalog
	Codec   encoding.Codec
}

// Truncate deletes all the documents from the table.
func (t *Table) Truncate() error {
	return t.Store.Truncate()
}

// Insert the document into the table.
// If a primary key has been specified during the table creation, the field is expected to be present
// in the given document.
// If no primary key has been selected, a monotonic autoincremented integer key will be generated.
// It returns the inserted document alongside its key.
func (t *Table) Insert(d types.Document) ([]byte, types.Document, error) {
	return t.InsertWithConflictResolution(d, nil)
}

// This function must be atomic, i.e either everything works or nothing does.
// In case there is an error, there are two solutions:
// - we return it and and rollback: any write done prior to writing to the store will be rolled back
// - we run the confliced resolution function, if provided
// In this case, we can't rely on a rollback and might end up with a dirty state (ex. document written to the store,
// but not to all indexes)
// To avoid that, we must first ensure there are no conflict (duplicate primary keys, unique constraints violation, etc.),
// run the conflict resolution function if needed and then start writing to the engine.
func (t *Table) InsertWithConflictResolution(d types.Document, onConflict OnInsertConflictAction) ([]byte, types.Document, error) {
	if t.Info.ReadOnly {
		return nil, nil, errors.New("cannot write to read-only table")
	}

	fb, err := t.Info.ValidateDocument(t.Tx, d)
	if err != nil {
		if onConflict != nil {
			if ce, ok := err.(*ConstraintViolationError); ok && ce.Constraint == "NOT NULL" {
				d, err := onConflict(t, nil, d, err)
				return nil, d, err
			}
		}
		return nil, nil, err
	}

	key, err := t.generateKey(t.Info, d)
	if err != nil {
		return nil, nil, err
	}

	// ensure the key is not already present in the table
	_, err = t.Store.Get(key)
	if err == nil {
		if onConflict != nil {
			d, err := onConflict(t, key, d, err)
			return key, d, err
		}

		return nil, nil, errs.ErrDuplicateDocument
	}

	indexes, err := t.GetIndexes()
	if err != nil {
		return nil, nil, err
	}

	// ensure there is no index violation
	for _, idx := range indexes {
		// only check unique indexes
		if !idx.Info.Unique {
			continue
		}

		vs := make([]types.Value, 0, len(idx.Info.Paths))

		for _, path := range idx.Info.Paths {
			v, err := path.GetValueFromDocument(fb)
			if err != nil {
				v = types.NewNullValue()
			}

			vs = append(vs, v)
		}

		duplicate, dKey, err := idx.Exists(vs)
		if err != nil {
			return nil, nil, err
		}
		if duplicate {
			if onConflict != nil {
				d, err := onConflict(t, dKey, d, err)
				return key, d, err
			}

			return nil, nil, errs.ErrDuplicateDocument
		}
	}

	// insert into the table
	var buf bytes.Buffer
	enc := t.Codec.NewEncoder(&buf)
	defer enc.Close()
	err = enc.EncodeDocument(fb)
	if err != nil {
		return nil, nil, stringutil.Errorf("failed to encode document: %w", err)
	}

	err = t.Store.Put(key, buf.Bytes())
	if err != nil {
		return nil, nil, err
	}

	// update indexes
	for _, idx := range indexes {
		vs := make([]types.Value, 0, len(idx.Info.Paths))

		for _, path := range idx.Info.Paths {
			v, err := path.GetValueFromDocument(fb)
			if err != nil {
				v = types.NewNullValue()
			}

			vs = append(vs, v)
		}

		err = idx.Set(vs, key)
		if err != nil {
			return nil, nil, err
		}
	}

	return key, fb, nil
}

// GetIndexes returns all indexes of the table.
func (t *Table) GetIndexes() ([]*Index, error) {
	if t.Indexes != nil {
		return t.Indexes, nil
	}

	names := t.Catalog.ListIndexes(t.Info.TableName)
	indexes := make([]*Index, 0, len(names))
	for _, idxName := range names {
		idx, err := t.Catalog.GetIndex(t.Tx, idxName)
		if err != nil {
			return nil, err
		}
		indexes = append(indexes, idx)
	}

	t.Indexes = indexes
	return indexes, nil
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

	indexes, err := t.GetIndexes()
	if err != nil {
		return err
	}

	for _, idx := range indexes {
		vs := make([]types.Value, 0, len(idx.Info.Paths))
		for _, path := range idx.Info.Paths {
			v, err := path.GetValueFromDocument(d)
			if err != nil {
				if errors.Is(err, document.ErrFieldNotFound) {
					v = types.NewNullValue()
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
func (t *Table) Replace(key []byte, d types.Document) (types.Document, error) {
	if t.Info.ReadOnly {
		return nil, errors.New("cannot write to read-only table")
	}

	d, err := t.Info.ValidateDocument(t.Tx, d)
	if err != nil {
		return nil, err
	}

	return d, t.replace(key, d)
}

func (t *Table) replace(key []byte, d types.Document) error {
	// make sure key exists
	old, err := t.GetDocument(key)
	if err != nil {
		return err
	}

	indexes, err := t.GetIndexes()
	if err != nil {
		return err
	}

	// remove key from indexes
	for _, idx := range indexes {
		vs := make([]types.Value, 0, len(idx.Info.Paths))
		for _, path := range idx.Info.Paths {
			v, err := path.GetValueFromDocument(old)
			if err != nil {
				v = types.NewNullValue()
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
	enc := t.Codec.NewEncoder(&buf)
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
		vs := make([]types.Value, 0, len(idx.Info.Paths))
		for _, path := range idx.Info.Paths {
			v, err := path.GetValueFromDocument(d)
			if err != nil {
				v = types.NewNullValue()
			}

			vs = append(vs, v)
		}

		err = idx.Set(vs, key)
		if err != nil {
			if errors.Is(err, ErrIndexDuplicateValue) {
				return errs.ErrDuplicateDocument
			}

			return err
		}
	}

	return nil
}

// This document implementation waits until
// GetByField or Iterate are called to
// fetch the value from the engine store.
// This is useful to prevent reading the value
// from store on documents that don't need to be
// decoded.
type lazilyDecodedDocument struct {
	item    engine.Item
	buf     []byte
	codec   encoding.Codec
	decoder encoding.Decoder
	dirty   bool
}

func (d *lazilyDecodedDocument) GetByField(field string) (v types.Value, err error) {
	if d.dirty {
		d.dirty = false
		err = d.copyFromItem()
		if err != nil {
			return
		}

		if d.decoder == nil {
			d.decoder = d.codec.NewDecoder(d.buf)
		} else {
			d.decoder.Reset(d.buf)
		}
	}

	return d.decoder.GetByField(field)
}

func (d *lazilyDecodedDocument) Iterate(fn func(field string, value types.Value) error) error {
	if d.dirty {
		d.dirty = false
		err := d.copyFromItem()
		if err != nil {
			return err
		}

		if d.decoder == nil {
			d.decoder = d.codec.NewDecoder(d.buf)
		} else {
			d.decoder.Reset(d.buf)
		}
	}

	return d.decoder.Iterate(fn)
}

func (d *lazilyDecodedDocument) Reset() {
	d.dirty = true
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
func (t *Table) Iterate(fn func(key []byte, d types.Document) error) error {
	return t.AscendGreaterOrEqual(nil, fn)
}

// EncodeValue encodes a value following primary key constraints.
// It returns a binary representation of the key as used in the store.
// It can be used to manually add a new entry to the store or to compare
// with other keys during table iteration.
func (t *Table) EncodeValue(v types.Value) ([]byte, error) {
	return t.encodeValueToKey(t.Info, v)
}

func (t *Table) encodeValueToKey(info *TableInfo, v types.Value) ([]byte, error) {
	var err error

	pk := t.Info.GetPrimaryKey()
	if pk == nil {
		// if no primary key was defined, convert the pivot to an integer then to an unsigned integer
		// and encode it as a varint
		v, err = document.CastAsInteger(v)
		if err != nil {
			return nil, err
		}

		docid := uint64(v.V().(int64))

		buf := make([]byte, binary.MaxVarintLen64)
		n := binary.PutUvarint(buf, docid)
		return buf[:n], nil
	}

	// if a primary key was defined and the primary is typed, convert the value to the right type.
	if !pk.Type.IsAny() {
		v, err = document.CastAs(v, pk.Type)
		if err != nil {
			return nil, err
		}
		// it no primary key type is specified,
		// and the value to encode is an integer
		// convert it to a double.
	} else if v.Type() == types.IntegerValue {
		v, err = document.CastAsDouble(v)
		if err != nil {
			return nil, err
		}
	}

	var buf bytes.Buffer
	err = tencoding.NewValueEncoder(&buf).Encode(v)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// AscendGreaterOrEqual iterates over the documents of the table whose key
// is greater than or equal to the pivot.
// The pivot is converted to the type of the primary key, if any, prior to iteration.
// If the pivot is empty, it iterates from the beginning of the table.
func (t *Table) AscendGreaterOrEqual(pivot types.Value, fn func(key []byte, d types.Document) error) error {
	return t.iterate(pivot, false, fn)
}

// DescendLessOrEqual iterates over the documents of the table whose key
// is less than or equal to the pivot, in reverse order.
// The pivot is converted to the type of the primary key, if any, prior to iteration.
// If the pivot is empty, it iterates from the end of the table in reverse order.
func (t *Table) DescendLessOrEqual(pivot types.Value, fn func(key []byte, d types.Document) error) error {
	return t.iterate(pivot, true, fn)
}

func (t *Table) iterate(pivot types.Value, reverse bool, fn func(key []byte, d types.Document) error) error {
	var seek []byte

	// if there is a pivot, convert it to the right type
	if pivot != nil && pivot.V() != nil {
		var err error
		seek, err = t.encodeValueToKey(t.Info, pivot)
		if err != nil {
			return err
		}
	}

	// To avoid unnecessary allocations, we create the struct once and reuse
	// it during each iteration.
	d := lazilyDecodedDocument{
		codec: t.Codec,
	}

	it := t.Store.Iterator(engine.IteratorOptions{Reverse: reverse})
	defer it.Close()

	for it.Seek(seek); it.Valid(); it.Next() {
		d.Reset()
		d.item = it.Item()
		// d must be passed as pointer, not value,
		// because passing a value to an interface
		// requires an allocation, while it doesn't for a pointer.
		err := fn(d.item.Key(), &d)
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
func (t *Table) GetDocument(key []byte) (types.Document, error) {
	v, err := t.Store.Get(key)
	if err != nil {
		if errors.Is(err, engine.ErrKeyNotFound) {
			return nil, errors.Wrap(errs.ErrDocumentNotFound)
		}
		return nil, stringutil.Errorf("failed to fetch document %q: %w", key, err)
	}

	return t.Codec.NewDecoder(v), err
}

// generate a key for d based on the table configuration.
// if the table has a primary key, it extracts the field from
// the document, converts it to the targeted type and returns
// its encoded version.
// if there are no primary key in the table, a default
// key is generated, called the docid.
func (t *Table) generateKey(info *TableInfo, d types.Document) ([]byte, error) {
	if pk := t.Info.GetPrimaryKey(); pk != nil {
		v, err := pk.Path.GetValueFromDocument(d)
		if errors.Is(err, document.ErrFieldNotFound) {
			return nil, stringutil.Errorf("missing primary key at path %q", pk.Path)
		}
		if err != nil {
			return nil, err
		}

		var buf bytes.Buffer
		err = tencoding.NewValueEncoder(&buf).Encode(v)
		if err != nil {
			return nil, err
		}
		return buf.Bytes(), nil
	}

	seq, err := t.Catalog.GetSequence(t.Info.DocidSequenceName)
	if err != nil {
		return nil, err
	}
	docid, err := seq.Next(t.Tx, t.Catalog)
	if err != nil {
		return nil, err
	}

	buf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(buf, uint64(docid))
	return buf[:n], nil
}
