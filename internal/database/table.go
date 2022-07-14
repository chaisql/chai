package database

import (
	"fmt"

	"github.com/cockroachdb/errors"
	"github.com/genjidb/genji/document"
	errs "github.com/genjidb/genji/internal/errors"
	"github.com/genjidb/genji/internal/kv"
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
func (t *Table) Insert(d types.Document) (*tree.Key, types.Document, error) {
	if t.Info.ReadOnly {
		return nil, nil, errors.New("cannot write to read-only table")
	}

	key, err := t.generateKey(t.Info, d)
	if err != nil {
		return nil, nil, err
	}

	d, enc, err := t.encodeDocument(d)
	if err != nil {
		return nil, nil, err
	}

	// insert into the table
	err = t.Tree.Insert(key, enc)
	if err != nil {
		if errors.Is(err, kv.ErrKeyAlreadyExists) {
			return nil, nil, &ConstraintViolationError{
				Constraint: "PRIMARY KEY",
				Paths:      t.Info.GetPrimaryKey().Paths,
				Key:        key,
			}
		}

		return nil, nil, errors.Wrapf(err, "failed to insert document %q", key)
	}

	return key, d, nil
}

func (t *Table) encodeDocument(d types.Document) (types.Document, []byte, error) {
	ed, ok := d.(*EncodedDocument)
	if ok {
		return d, ed.encoded, nil
	}

	dst, err := t.Info.EncodeDocument(t.Tx, nil, d)
	if err != nil {
		return nil, nil, err
	}
	return NewEncodedDocument(&t.Info.FieldConstraints, dst), dst, nil
}

// Delete a document by key.
func (t *Table) Delete(key *tree.Key) error {
	if t.Info.ReadOnly {
		return errors.New("cannot write to read-only table")
	}

	err := t.Tree.Delete(key)
	if errors.Is(err, kv.ErrKeyNotFound) {
		return errors.WithStack(errs.NewNotFoundError(key.String()))
	}

	return err
}

// Replace a document by key.
// An error is returned if the key doesn't exist.
func (t *Table) Replace(key *tree.Key, d types.Document) (types.Document, error) {
	if t.Info.ReadOnly {
		return nil, errors.New("cannot write to read-only table")
	}

	// make sure key exists
	ok, err := t.Tree.Exists(key)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, errors.Wrapf(errs.NewNotFoundError(key.String()), "can't replace key %q", key)
	}

	d, enc, err := t.encodeDocument(d)
	if err != nil {
		return nil, err
	}

	// replace old document with new document
	err = t.Tree.Put(key, enc)
	return d, err
}

func (t *Table) IterateOnRange(rng *Range, reverse bool, fn func(key *tree.Key, d types.Document) error) error {
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

	e := EncodedDocument{
		fieldConstraints: &t.Info.FieldConstraints,
	}

	return t.Tree.IterateOnRange(r, reverse, func(k *tree.Key, enc []byte) error {
		e.encoded = enc
		return fn(k, NewEncodedDocument(&t.Info.FieldConstraints, enc))
	})
}

// GetDocument returns one document by key.
func (t *Table) GetDocument(key *tree.Key) (types.Document, error) {
	enc, err := t.Tree.Get(key)
	if err != nil {
		if errors.Is(err, kv.ErrKeyNotFound) {
			return nil, errors.WithStack(errs.NewNotFoundError(key.String()))
		}
		return nil, fmt.Errorf("failed to fetch document %q: %w", key, err)
	}

	return NewEncodedDocument(&t.Info.FieldConstraints, enc), nil
}

// generate a key for d based on the table configuration.
// if the table has a primary key, it extracts the field from
// the document, converts it to the targeted type and returns
// its encoded version.
// if there are no primary key in the table, a default
// key is generated, called the docid.
func (t *Table) generateKey(info *TableInfo, d types.Document) (*tree.Key, error) {
	if pk := t.Info.GetPrimaryKey(); pk != nil {
		vs := make([]types.Value, 0, len(pk.Paths))
		for _, p := range pk.Paths {
			v, err := p.GetValueFromDocument(d)
			if errors.Is(err, types.ErrFieldNotFound) {
				return nil, fmt.Errorf("missing primary key at path %q", p)
			}
			if err != nil {
				return nil, err
			}

			vs = append(vs, v)
		}

		return tree.NewKey(vs...), nil
	}

	seq, err := t.Catalog.GetSequence(t.Info.DocidSequenceName)
	if err != nil {
		return nil, err
	}
	docid, err := seq.Next(t.Tx, t.Catalog)
	if err != nil {
		return nil, err
	}

	return tree.NewKey(types.NewIntegerValue(docid)), nil
}
