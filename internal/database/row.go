package database

import (
	"github.com/chaisql/chai/internal/tree"
	"github.com/chaisql/chai/internal/types"
)

type Row interface {
	// Iterate goes through all the fields of the row and calls the given function
	// by passing the column name
	Iterate(fn func(column string, value types.Value) error) error

	// Get returns the value of the given column.
	// If the column does not exist, it returns ErrColumnNotFound.
	Get(name string) (types.Value, error)

	// TableName returns the name of the table the row belongs to.
	TableName() string

	// MarshalJSON encodes the row as JSON.
	MarshalJSON() ([]byte, error)

	// Key returns the row key.
	Key() *tree.Key

	Object() types.Object
}

var _ Row = (*LazyRow)(nil)

// LazyRow holds an LazyRow key and lazily loads the LazyRow on demand when the Iterate or GetByField method is called.
// It implements the Row and the object.Keyer interfaces.
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
	return r.GetByField(name)
}

func (r *LazyRow) GetByField(field string) (types.Value, error) {
	var err error
	if r.row == nil {
		r.row, err = r.table.GetRow(r.key)
		if err != nil {
			return nil, err
		}
	}

	return r.row.Get(field)
}

func (r *LazyRow) MarshalJSON() ([]byte, error) {
	if r.row == nil {
		var err error
		r.row, err = r.table.GetRow(r.key)
		if err != nil {
			return nil, err
		}
	}

	return r.row.(interface{ MarshalJSON() ([]byte, error) }).MarshalJSON()
}

func (r *LazyRow) Key() *tree.Key {
	return r.key
}

func (r *LazyRow) TableName() string {
	return r.table.Info.TableName
}

func (r *LazyRow) Object() types.Object {
	if r.row == nil {
		var err error
		r.row, err = r.table.GetRow(r.key)
		if err != nil {
			panic(err)
		}
	}

	return r.row.Object()
}

var _ Row = (*BasicRow)(nil)

type BasicRow struct {
	tableName string
	key       *tree.Key
	obj       types.Object
}

func NewBasicRow(obj types.Object) *BasicRow {
	return &BasicRow{
		obj: obj,
	}
}

func (r *BasicRow) ResetWith(tableName string, key *tree.Key, obj types.Object) {
	r.tableName = tableName
	r.key = key
	r.obj = obj
}

func (r *BasicRow) Iterate(fn func(name string, value types.Value) error) error {
	return r.obj.Iterate(fn)
}

func (r *BasicRow) Get(name string) (types.Value, error) {
	return r.obj.GetByField(name)
}

func (r *BasicRow) MarshalJSON() ([]byte, error) {
	return r.obj.(interface{ MarshalJSON() ([]byte, error) }).MarshalJSON()
}

func (r *BasicRow) Key() *tree.Key {
	return r.key
}

func (r *BasicRow) TableName() string {
	return r.tableName
}

func (r *BasicRow) Object() types.Object {
	return r.obj
}

type RowIterator interface {
	// Iterate goes through all the rows of the table and calls the given function by passing each one of them.
	// If the given function returns an error, the iteration stops.
	Iterate(fn func(Row) error) error
}
