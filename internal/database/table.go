package database

import (
	"fmt"

	"github.com/chaisql/chai/internal/engine"
	errs "github.com/chaisql/chai/internal/errors"
	"github.com/chaisql/chai/internal/row"
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
// in the given row.
// If no primary key has been selected, a monotonic autoincremented integer key will be generated.
// It returns the inserted object alongside its key.
func (t *Table) Insert(r row.Row) (*tree.Key, Row, error) {
	if t.Info.ReadOnly {
		return nil, nil, errors.New("cannot write to read-only table")
	}

	key, err := t.GenerateKey(r)
	if err != nil {
		return nil, nil, err
	}

	r, enc, err := t.encodeRow(r)
	if err != nil {
		return nil, nil, err
	}

	// insert into the table
	err = t.Tree.Insert(key, enc)
	if err != nil {
		if errors.Is(err, engine.ErrKeyAlreadyExists) {
			return nil, nil, &ConstraintViolationError{
				Constraint: "PRIMARY KEY",
				Columns:    t.Info.PrimaryKey.Columns,
				Key:        key,
			}
		}

		return nil, nil, errors.Wrapf(err, "failed to insert row %q", key)
	}

	return key, &BasicRow{
		tableName: t.Info.TableName,
		Row:       r,
		key:       key,
	}, nil
}

func (t *Table) encodeRow(r row.Row) (row.Row, []byte, error) {
	ed, ok := r.(*EncodedRow)
	// pointer comparison is enough here
	if ok && ed.columnConstraints == &t.Info.ColumnConstraints {
		return r, ed.encoded, nil
	}

	dst, err := t.Info.EncodeRow(t.Tx, nil, r)
	if err != nil {
		return nil, nil, err
	}

	return NewEncodedRow(&t.Info.ColumnConstraints, dst), dst, nil
}

// Delete a object by key.
func (t *Table) Delete(key *tree.Key) error {
	if t.Info.ReadOnly {
		return errors.New("cannot write to read-only table")
	}

	err := t.Tree.Delete(key)
	if errors.Is(err, engine.ErrKeyNotFound) {
		return errs.NewNotFoundError(key.String())
	}

	return err
}

// Replace a row by key.
// An error is returned if the key doesn't exist.
func (t *Table) Replace(key *tree.Key, r row.Row) (Row, error) {
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

	return t.Put(key, r)
}

// Exists checks if a row exists by key.
func (t *Table) Exists(key *tree.Key) (bool, error) {
	// make sure key exists
	return t.Tree.Exists(key)
}

// Put a row by key
func (t *Table) Put(key *tree.Key, r row.Row) (Row, error) {
	if t.Info.ReadOnly {
		return nil, errors.New("cannot write to read-only table")
	}

	r, enc, err := t.encodeRow(r)
	if err != nil {
		return nil, err
	}

	// replace old row with new row
	err = t.Tree.Put(key, enc)
	if err != nil {
		return nil, err
	}

	return &BasicRow{
		tableName: t.Info.TableName,
		Row:       r,
		key:       key,
	}, nil
}

func (t *Table) Iterator(rng *Range) (*TableIterator, error) {
	var columns []string

	pk := t.Info.PrimaryKey
	if pk != nil {
		columns = pk.Columns
	}

	var r *tree.Range
	var err error

	if rng != nil {
		r, err = rng.ToTreeRange(&t.Info.ColumnConstraints, columns)
		if err != nil {
			return nil, err
		}
	}

	it, err := t.Tree.Iterator(r)
	if err != nil {
		return nil, err
	}

	return newIterator(it, t.Info.TableName, &t.Info.ColumnConstraints), nil
}

// GetRow returns one row by key.
func (t *Table) GetRow(key *tree.Key) (Row, error) {
	enc, err := t.Tree.Get(key)
	if err != nil {
		if errors.Is(err, engine.ErrKeyNotFound) {
			return nil, errs.NewNotFoundError(key.String())
		}
		return nil, fmt.Errorf("failed to fetch row %q: %w", key, err)
	}

	return &BasicRow{
		tableName: t.Info.TableName,
		Row:       NewEncodedRow(&t.Info.ColumnConstraints, enc),
		key:       key,
	}, nil
}

// GenerateKey generates a key for o based on the table configuration.
// It returns a boolean indicating whether the key is a rowid or not.
func (t *Table) GenerateKey(r row.Row) (*tree.Key, error) {
	pk := t.Info.PrimaryKey
	vs := make([]types.Value, 0, len(pk.Columns))
	for _, c := range pk.Columns {
		v, err := r.Get(c)
		if errors.Is(err, types.ErrColumnNotFound) {
			return nil, fmt.Errorf("missing primary key at path %q", c)
		}
		if err != nil {
			return nil, err
		}

		vs = append(vs, v)
	}

	return tree.NewKey(vs...), nil
}
