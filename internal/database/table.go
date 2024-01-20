package database

import (
	"fmt"

	"github.com/chaisql/chai/internal/engine"
	errs "github.com/chaisql/chai/internal/errors"
	"github.com/chaisql/chai/internal/object"
	"github.com/chaisql/chai/internal/tree"
	"github.com/chaisql/chai/internal/types"
	"github.com/cockroachdb/errors"
)

// A Table represents a collection of objects.
type Table struct {
	Tx   *Transaction
	Tree *tree.Tree
	// Table information.
	// May not represent the most up to date data.
	// Always get a fresh Table instance before relying on this field.
	Info *TableInfo
}

// Truncate deletes all the objects from the table.
func (t *Table) Truncate() error {
	return t.Tree.Truncate()
}

// Insert the object into the table.
// If a primary key has been specified during the table creation, the field is expected to be present
// in the given object.
// If no primary key has been selected, a monotonic autoincremented integer key will be generated.
// It returns the inserted object alongside its key.
func (t *Table) Insert(o types.Object) (*tree.Key, Row, error) {
	if t.Info.ReadOnly {
		return nil, nil, errors.New("cannot write to read-only table")
	}

	key, isRowid, err := t.generateKey(t.Info, o)
	if err != nil {
		return nil, nil, err
	}

	o, enc, err := t.encodeObject(o)
	if err != nil {
		return nil, nil, err
	}

	// insert into the table
	if !isRowid {
		// if the key is not a rowid, make sure it doesn't exist
		// by using Insert instead of Put
		err = t.Tree.Insert(key, enc)
	} else {
		err = t.Tree.Put(key, enc)
	}
	if err != nil {
		if errors.Is(err, engine.ErrKeyAlreadyExists) {
			return nil, nil, &ConstraintViolationError{
				Constraint: "PRIMARY KEY",
				Paths:      t.Info.PrimaryKey.Paths,
				Key:        key,
			}
		}

		return nil, nil, errors.Wrapf(err, "failed to insert row %q", key)
	}

	return key, &BasicRow{
		tableName: t.Info.TableName,
		obj:       o,
		key:       key,
	}, nil
}

func (t *Table) encodeObject(o types.Object) (types.Object, []byte, error) {
	ed, ok := o.(*EncodedObject)
	// pointer comparison is enough here
	if ok && ed.fieldConstraints == &t.Info.FieldConstraints {
		return o, ed.encoded, nil
	}

	dst, err := t.Info.EncodeObject(t.Tx, nil, o)
	if err != nil {
		return nil, nil, err
	}

	return NewEncodedObject(&t.Info.FieldConstraints, dst), dst, nil
}

// Delete a object by key.
func (t *Table) Delete(key *tree.Key) error {
	if t.Info.ReadOnly {
		return errors.New("cannot write to read-only table")
	}

	err := t.Tree.Delete(key)
	if errors.Is(err, engine.ErrKeyNotFound) {
		return errors.WithStack(errs.NewNotFoundError(key.String()))
	}

	return err
}

// Replace a row by key.
// An error is returned if the key doesn't exist.
func (t *Table) Replace(key *tree.Key, o types.Object) (Row, error) {
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

	return t.Put(key, o)
}

// Put a row by key. If the key doesn't exist, it is created.
func (t *Table) Put(key *tree.Key, o types.Object) (Row, error) {
	if t.Info.ReadOnly {
		return nil, errors.New("cannot write to read-only table")
	}

	o, enc, err := t.encodeObject(o)
	if err != nil {
		return nil, err
	}

	// replace old row with new row
	err = t.Tree.Put(key, enc)
	return &BasicRow{
		tableName: t.Info.TableName,
		obj:       o,
		key:       key,
	}, err
}

func (t *Table) IterateOnRange(rng *Range, reverse bool, fn func(key *tree.Key, r Row) error) error {
	e := EncodedObject{
		fieldConstraints: &t.Info.FieldConstraints,
	}
	row := BasicRow{
		tableName: t.Info.TableName,
		obj:       &e,
	}

	return t.IterateRawOnRange(rng, reverse, func(k *tree.Key, enc []byte) error {
		row.key = k
		e.encoded = enc
		return fn(k, &row)
	})
}

func (t *Table) IterateRawOnRange(rng *Range, reverse bool, fn func(key *tree.Key, row []byte) error) error {
	var paths []object.Path

	pk := t.Info.PrimaryKey
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

	return t.Tree.IterateOnRange(r, reverse, fn)
}

// GetRow returns one row by key.
func (t *Table) GetRow(key *tree.Key) (Row, error) {
	enc, err := t.Tree.Get(key)
	if err != nil {
		if errors.Is(err, engine.ErrKeyNotFound) {
			return nil, errors.WithStack(errs.NewNotFoundError(key.String()))
		}
		return nil, fmt.Errorf("failed to fetch row %q: %w", key, err)
	}

	return &BasicRow{
		tableName: t.Info.TableName,
		obj:       NewEncodedObject(&t.Info.FieldConstraints, enc),
		key:       key,
	}, nil
}

// generate a key for o based on the table configuration.
// if the table has a primary key, it extracts the field from
// the object, converts it to the targeted type and returns
// its encoded version.
// if there are no primary key in the table, a default
// key is generated, called the rowid.
// It returns a boolean indicating whether the key is a rowid or not.
func (t *Table) generateKey(info *TableInfo, o types.Object) (*tree.Key, bool, error) {
	if pk := t.Info.PrimaryKey; pk != nil {
		vs := make([]types.Value, 0, len(pk.Paths))
		for _, p := range pk.Paths {
			v, err := p.GetValueFromObject(o)
			if errors.Is(err, types.ErrFieldNotFound) {
				return nil, false, fmt.Errorf("missing primary key at path %q", p)
			}
			if err != nil {
				return nil, false, err
			}

			vs = append(vs, v)
		}

		return tree.NewKey(vs...), false, nil
	}

	seq, err := t.Tx.Catalog.GetSequence(t.Info.RowidSequenceName)
	if err != nil {
		return nil, true, err
	}
	rowid, err := seq.Next(t.Tx)
	if err != nil {
		return nil, true, err
	}

	return tree.NewKey(types.NewIntegerValue(rowid)), true, nil
}
