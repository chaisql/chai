package database

import (
	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/document/encoding"
	"github.com/genjidb/genji/engine"
	errs "github.com/genjidb/genji/errors"
	"github.com/genjidb/genji/internal/errors"
	"github.com/genjidb/genji/internal/stringutil"
	"github.com/genjidb/genji/internal/tree"
	"github.com/genjidb/genji/types"
)

// A Table represents a collection of documents.
type Table struct {
	Tx   *Transaction
	Tree *tree.Tree
	// Table information.
	// May not represent the most up to date data.
	// Always get a fresh Table instance before relying on this field.
	Info *TableInfo

	Catalog *Catalog
	Codec   encoding.Codec
}

// Truncate deletes all the documents from the table.
func (t *Table) Truncate() error {
	return t.Tree.Truncate()
}

// Insert the document into the table.
// If a primary key has been specified during the table creation, the field is expected to be present
// in the given document.
// If no primary key has been selected, a monotonic autoincremented integer key will be generated.
// It returns the inserted document alongside its key.
func (t *Table) Insert(d types.Document) (tree.Key, types.Document, error) {
	if t.Info.ReadOnly {
		return nil, nil, errors.New("cannot write to read-only table")
	}

	key, err := t.generateKey(t.Info, d)
	if err != nil {
		return nil, nil, err
	}

	// ensure the key is not already present in the table
	_, err = t.Tree.Get(key)
	if err == nil {
		return nil, nil, &errs.ConstraintViolationError{
			Constraint: "PRIMARY KEY",
			Paths:      t.Info.GetPrimaryKey().Paths,
			Key:        key,
		}
	}

	// insert into the table
	err = t.Tree.Put(key, types.NewDocumentValue(d))
	if err != nil {
		return nil, nil, err
	}

	return key, d, nil
}

// Delete a document by key.
func (t *Table) Delete(key tree.Key) error {
	if t.Info.ReadOnly {
		return errors.New("cannot write to read-only table")
	}

	err := t.Tree.Delete(key)
	if errors.Is(err, engine.ErrKeyNotFound) {
		return errs.ErrDocumentNotFound
	}

	return err
}

// Replace a document by key.
// An error is returned if the key doesn't exist.
// Indexes are automatically updated.
func (t *Table) Replace(key tree.Key, d types.Document) (types.Document, error) {
	if t.Info.ReadOnly {
		return nil, errors.New("cannot write to read-only table")
	}

	// make sure key exists
	_, err := t.Tree.Get(key)
	if err != nil {
		if errors.Is(err, engine.ErrKeyNotFound) {
			return nil, errs.ErrDocumentNotFound
		}

		return nil, err
	}

	// replace old document with new document
	err = t.Tree.Put(key, types.NewDocumentValue(d))
	return d, err
}

// This document implementation waits until
// GetByField or Iterate are called to
// fetch the value from the engine store.
// This is useful to prevent reading the value
// from store on documents that don't need to be
// decoded.
type lazilyDecodedDocument struct {
	types.Value
}

func (d *lazilyDecodedDocument) GetByField(field string) (v types.Value, err error) {
	doc := d.V().(types.Document)
	return doc.GetByField(field)
}

func (d *lazilyDecodedDocument) Iterate(fn func(field string, value types.Value) error) error {
	doc := d.V().(types.Document)
	return doc.Iterate(fn)
}

func (t *Table) IterateOnRange(rng *Range, reverse bool, fn func(key tree.Key, d types.Document) error) error {
	var paths []document.Path

	pk := t.Info.GetPrimaryKey()
	if pk != nil {
		paths = pk.Paths
	}

	var r *tree.Range
	var err error

	if rng != nil {
		r, err = rng.ToTreeRange(&t.Info.FieldConstraints, paths)
		if err != nil {
			return err
		}
	}

	var d lazilyDecodedDocument

	return t.Tree.IterateOnRange(r, reverse, func(k tree.Key, v types.Value) error {
		d.Value = v
		return fn(k, &d)
	})
}

// GetDocument returns one document by key.
func (t *Table) GetDocument(key tree.Key) (types.Document, error) {
	v, err := t.Tree.Get(key)
	if err != nil {
		if errors.Is(err, engine.ErrKeyNotFound) {
			return nil, errors.Wrap(errs.ErrDocumentNotFound)
		}
		return nil, stringutil.Errorf("failed to fetch document %q: %w", key, err)
	}

	return &lazilyDecodedDocument{v}, nil
}

// generate a key for d based on the table configuration.
// if the table has a primary key, it extracts the field from
// the document, converts it to the targeted type and returns
// its encoded version.
// if there are no primary key in the table, a default
// key is generated, called the docid.
func (t *Table) generateKey(info *TableInfo, d types.Document) (tree.Key, error) {
	if pk := t.Info.GetPrimaryKey(); pk != nil {
		vs := make([]types.Value, 0, len(pk.Paths))
		for _, p := range pk.Paths {
			v, err := p.GetValueFromDocument(d)
			if errors.Is(err, document.ErrFieldNotFound) {
				return nil, stringutil.Errorf("missing primary key at path %q", p)
			}
			if err != nil {
				return nil, err
			}

			vs = append(vs, v)
		}

		return tree.NewKey(vs...)
	}

	seq, err := t.Catalog.GetSequence(t.Info.DocidSequenceName)
	if err != nil {
		return nil, err
	}
	docid, err := seq.Next(t.Tx, t.Catalog)
	if err != nil {
		return nil, err
	}

	return tree.NewKey(types.NewIntegerValue(docid))
}
