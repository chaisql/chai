package database

import (
	"github.com/chaisql/chai/internal/row"
	"github.com/chaisql/chai/internal/tree"
	"github.com/chaisql/chai/internal/types"
)

type Row interface {
	// Iterate goes through all the columns of the row and calls the given function
	// by passing the column name
	Iterate(fn func(column string, value types.Value) error) error

	// Get returns the value of the given column.
	// If the column does not exist, it returns ErrColumnNotFound.
	Get(name string) (types.Value, error)

	// TableName returns the name of the table the row belongs to.
	TableName() string

	// Key returns the row key.
	Key() *tree.Key
}

var _ Row = (*LazyRow)(nil)

// LazyRow holds an LazyRow key and lazily loads the LazyRow on demand when the Iterate or Get method is called.
// It implements the Row and the row.Keyer interfaces.
type LazyRow struct {
	key   *tree.Key
	table *Table
	row   Row
}

func (r *LazyRow) ResetWith(table *Table, key *tree.Key) {
	r.key = key
	r.table = table
	r.row = nil
}

func (r *LazyRow) Iterate(fn func(name string, value types.Value) error) error {
	var err error
	if r.row == nil {
		r.row, err = r.table.GetRow(r.key)
		if err != nil {
			return err
		}
	}

	return r.row.Iterate(fn)
}

func (r *LazyRow) Get(name string) (types.Value, error) {
	var err error
	if r.row == nil {
		r.row, err = r.table.GetRow(r.key)
		if err != nil {
			return nil, err
		}
	}

	return r.row.Get(name)
}

func (r *LazyRow) Key() *tree.Key {
	return r.key
}

func (r *LazyRow) TableName() string {
	return r.table.Info.TableName
}

var _ Row = (*BasicRow)(nil)

type BasicRow struct {
	row.Row
	tableName   string
	key         *tree.Key
	originalRow Row
}

func NewBasicRow(r row.Row) *BasicRow {
	return &BasicRow{
		Row: r,
	}
}

func (r *BasicRow) ResetWith(tableName string, key *tree.Key, rr row.Row) {
	r.tableName = tableName
	r.key = key
	r.Row = rr
}

func (r *BasicRow) SetOriginalRow(original Row) {
	r.originalRow = original
}

func (r *BasicRow) OriginalRow() Row {
	if r.originalRow != nil {
		return r.originalRow
	}

	br, ok := r.Row.(*BasicRow)
	if ok {
		return br.OriginalRow()
	}

	return nil
}

func (r *BasicRow) Key() *tree.Key {
	return r.key
}

func (r *BasicRow) TableName() string {
	return r.tableName
}

type Result interface {
	// Iterator returns an iterator over the rows in the result.
	Iterator() (Iterator, error)
}

type Iterator interface {
	// Next moves the iterator to the next row.
	Next() bool

	// Row returns the current row.
	Row() (Row, error)

	// Close closes the iterator.
	Close() error

	// Error returns any error that occurred during iteration.
	Error() error
}
