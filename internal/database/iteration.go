package database

import (
	"github.com/chaisql/chai/internal/tree"
	"github.com/chaisql/chai/internal/types"
)

type Pivot []types.Value

type Range struct {
	Min, Max  Pivot
	Exclusive bool
	Exact     bool
}

func (r *Range) ToTreeRange(constraints *ColumnConstraints, columns []string) (*tree.Range, error) {
	var rng tree.Range

	if len(r.Min) > 0 {
		rng.Min = tree.NewKey(r.Min...)
	}

	if len(r.Max) > 0 {
		rng.Max = tree.NewKey(r.Max...)
	}

	if r.Exclusive && r.Exact {
		panic("exclusive and exact cannot both be true")
	}

	if r.Exact {
		if rng.Max != nil {
			panic("cannot use exact with a max range")
		}

		rng.Max = rng.Min
	}

	rng.Exclusive = r.Exclusive

	return &rng, nil
}

func (r *Range) IsEqual(other *Range) bool {
	if r.Exact != other.Exact {
		return false
	}

	if r.Exclusive != other.Exclusive {
		return false
	}

	if len(r.Min) != len(other.Min) {
		return false
	}

	if len(r.Max) != len(other.Max) {
		return false
	}

	for i := range r.Min {
		eq, err := r.Min[i].EQ(other.Min[i])
		if err != nil || !eq {
			return false
		}
	}

	for i := range r.Max {
		eq, err := r.Max[i].EQ(other.Max[i])
		if err != nil || !eq {
			return false
		}
	}

	return true
}

type TableIterator struct {
	*tree.Iterator
	e   EncodedRow
	row BasicRow
}

func newIterator(ti *tree.Iterator, tableName string, columnConstraints *ColumnConstraints) *TableIterator {
	it := TableIterator{
		Iterator: ti,
	}

	it.e.columnConstraints = columnConstraints
	it.row.tableName = tableName
	it.row.Row = &it.e

	return &it
}

func (it *TableIterator) Value() (Row, error) {
	var err error

	it.row.key = it.Iterator.Key()
	it.e.encoded, err = it.Iterator.Value()
	if err != nil {
		return nil, err
	}

	return &it.row, nil
}

type IndexIterator struct {
	*tree.Iterator
}

func (it *IndexIterator) Value() (*tree.Key, error) {
	k := it.Iterator.Key()
	// we don't care about the value, we just want to extract the key
	// which is the last element of the encoded array
	values, err := k.Decode()
	if err != nil {
		return nil, err
	}

	return tree.NewEncodedKey(types.AsByteSlice(values[len(values)-1])), nil
}
