package stream

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/genjidb/genji/database"
	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/sql/query/expr"
)

// An Iterator can iterate over values.
type Iterator interface {
	// Iterate goes through all the values and calls the given function by passing each one of them.
	// If the given function returns an error, the iteration stops.
	Iterate(fn func(env *expr.Environment) error) error
}

// The IteratorFunc type is an adapter to allow the use of ordinary functions as Iterators.
// If f is a function with the appropriate signature, IteratorFunc(f) is an Iterator that calls f.
type IteratorFunc func(fn func(env *expr.Environment) error) error

// Iterate calls f(fn).
func (f IteratorFunc) Iterate(fn func(env *expr.Environment) error) error {
	return f(fn)
}

type documentIterator []document.Document

func (it documentIterator) Iterate(fn func(env *expr.Environment) error) error {
	var env expr.Environment

	for _, d := range it {
		env.SetDocument(d)
		err := fn(&env)
		if err != nil {
			return err
		}
	}

	return nil
}

// NewDocumentIterator creates an iterator that iterates over the given values.
func NewDocumentIterator(documents ...document.Document) Iterator {
	return documentIterator(documents)
}

// A TableIterator iterates over the documents of a table.
type TableIterator struct {
	Name    string
	Table   *database.Table
	Params  []expr.Param
	Min     document.Value
	Max     document.Value
	Reverse bool
}

// NewTableIterator creates an iterator that iterates over each document of the given table.
func NewTableIterator(name string) *TableIterator {
	return &TableIterator{Name: name}
}

// TableIteratorOptions are used to control the iteration range and direction.
type TableIteratorOptions struct {
	Min     document.Value
	Max     document.Value
	Reverse bool
}

// NewTableIteratorWithOptions creates an iterator that iterates over each document of the given table.
func NewTableIteratorWithOptions(name string, opt TableIteratorOptions) *TableIterator {
	return &TableIterator{
		Name:    name,
		Min:     opt.Min,
		Max:     opt.Max,
		Reverse: opt.Reverse,
	}
}

// Bind the iterator to the table and parameters.
func (it *TableIterator) Bind(tx *database.Transaction, params []expr.Param) error {
	var err error

	it.Table, err = tx.GetTable(it.Name)
	if err != nil {
		return err
	}
	it.Params = params
	return nil
}

// Iterate over the documents of the table. Each document is stored in the environment
// that is passed to the fn function, using SetCurrentValue.
func (it *TableIterator) Iterate(fn func(env *expr.Environment) error) error {
	var env expr.Environment
	env.Params = it.Params

	var min, max []byte
	var err error

	if !it.Min.Type.IsZero() {
		min, err = it.Table.EncodeValueToKey(it.Min)
		if err != nil {
			return err
		}
	}

	if !it.Max.Type.IsZero() {
		max, err = it.Table.EncodeValueToKey(it.Max)
		if err != nil {
			return err
		}
	}

	errStop := errors.New("stop")

	if !it.Reverse {
		if max == nil {
			return it.Table.AscendGreaterOrEqual(it.Min, func(d document.Document) error {
				env.SetDocument(d)
				return fn(&env)
			})
		}
		err := it.Table.AscendGreaterOrEqual(it.Min, func(d document.Document) error {
			k := d.(document.Keyer).RawKey()

			// if there is an upper bound, iterate until we reach the max key
			if bytes.Compare(k, max) >= 0 {
				return errStop
			}

			env.SetDocument(d)
			return fn(&env)
		})
		if err == errStop {
			err = nil
		}
		return err
	}

	if min == nil {
		return it.Table.DescendLessOrEqual(it.Max, func(d document.Document) error {
			env.SetDocument(d)
			return fn(&env)
		})
	}

	err = it.Table.DescendLessOrEqual(it.Max, func(d document.Document) error {
		k := d.(document.Keyer).RawKey()

		// if there is a lower bound, iterate until we reach the min key
		if bytes.Compare(k, min) <= 0 {
			return errStop
		}

		env.SetDocument(d)
		return fn(&env)
	})
	if err == errStop {
		err = nil
	}
	return err
}

func (it *TableIterator) String() string {
	var min, max, reverse string
	if !it.Min.Type.IsZero() {
		min = it.Min.String()
	}
	if !it.Max.Type.IsZero() {
		max = it.Max.String()
	}

	reverse = "+"
	if it.Reverse {
		reverse = "-"
	}

	return fmt.Sprintf("%s%s[%s:%s]", reverse, it.Name, min, max)
}

// A IndexIterator iterates over the documents of an index.
type IndexIterator struct {
	Name    string
	Index   *database.Index
	Table   *database.Table
	Params  []expr.Param
	Min     document.Value
	Max     document.Value
	Reverse bool
}

// Bind the iterator to the index and parameters.
func (it *IndexIterator) Bind(tx *database.Transaction, params []expr.Param) error {
	var err error

	it.Index, err = tx.GetIndex(it.Name)
	if err != nil {
		return err
	}

	it.Table, err = tx.GetTable(it.Index.Opts.TableName)
	if err != nil {
		return err
	}
	it.Params = params
	return nil
}

// NewIndexIterator creates an iterator that iterates over each document of the given table.
func NewIndexIterator(name string) *IndexIterator {
	return &IndexIterator{Name: name}
}

// IndexIteratorOptions are used to control the iteration range and direction.
type IndexIteratorOptions struct {
	Min     document.Value
	Max     document.Value
	Reverse bool
}

// NewIndexIteratorWithOptions creates an iterator that iterates over each document of the given table.
func NewIndexIteratorWithOptions(name string, opt IndexIteratorOptions) *IndexIterator {
	return &IndexIterator{
		Name:    name,
		Min:     opt.Min,
		Max:     opt.Max,
		Reverse: opt.Reverse,
	}
}

func (it *IndexIterator) String() string {
	var min, max, reverse string
	if !it.Min.Type.IsZero() {
		min = it.Min.String()
	}
	if !it.Max.Type.IsZero() {
		max = it.Max.String()
	}

	reverse = "+"
	if it.Reverse {
		reverse = "-"
	}

	return fmt.Sprintf("%s%s[%s:%s]", reverse, it.Name, min, max)
}

// Iterate over the documents of the table. Each document is stored in the environment
// that is passed to the fn function, using SetCurrentValue.
func (it *IndexIterator) Iterate(fn func(env *expr.Environment) error) error {
	var env expr.Environment
	env.Params = it.Params

	var min, max []byte
	var err error

	if !it.Min.Type.IsZero() {
		min, err = it.Index.EncodeValue(it.Min)
		if err != nil {
			return err
		}
	}

	if !it.Max.Type.IsZero() {
		max, err = it.Index.EncodeValue(it.Max)
		if err != nil {
			return err
		}
	}

	errStop := errors.New("stop")

	if !it.Reverse {
		if max == nil {
			return it.Index.AscendGreaterOrEqual(it.Min, func(val, key []byte, isEqual bool) error {
				d, err := it.Table.GetDocument(key)
				if err != nil {
					return err
				}

				env.SetDocument(d)
				return fn(&env)
			})
		}
		err := it.Index.AscendGreaterOrEqual(it.Min, func(val, key []byte, isEqual bool) error {
			// if there is an upper bound, iterate until we reach the max key
			if bytes.Compare(val, max) >= 0 {
				return errStop
			}

			d, err := it.Table.GetDocument(key)
			if err != nil {
				return err
			}

			env.SetDocument(d)
			return fn(&env)
		})
		if err == errStop {
			err = nil
		}
		return err
	}

	if min == nil {
		return it.Index.DescendLessOrEqual(it.Max, func(val, key []byte, isEqual bool) error {
			d, err := it.Table.GetDocument(key)
			if err != nil {
				return err
			}

			env.SetDocument(d)
			return fn(&env)
		})
	}

	err = it.Index.DescendLessOrEqual(it.Max, func(val, key []byte, isEqual bool) error {
		// if there is a lower bound, iterate until we reach the min key
		if bytes.Compare(val, min) <= 0 {
			return errStop
		}

		d, err := it.Table.GetDocument(key)
		if err != nil {
			return err
		}
		env.SetDocument(d)
		return fn(&env)
	})
	if err == errStop {
		err = nil
	}
	return err
}
