package stream

import (
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
	Name     string
	Table    *database.Table
	Params   []expr.Param
	Operator expr.Operator
}

// NewTableIterator creats an iterator that iterates over each document of the given table.
func NewTableIterator(name string) *TableIterator {
	return &TableIterator{Name: name}
}

// TableIteratorOptions are used to control the iteration range and direction.
type TableIteratorOptions struct {
	Start, End document.Value
	Reverse    bool
}

// NewTableIteratorWithOptions creates an iterator that iterates over each document of the given table.
func NewTableIteratorWithOptions(name string, opt TableIteratorOptions) *TableIterator {
	return &TableIterator{Name: name}
}

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
	return it.Table.AscendGreaterOrEqual(func(d document.Document) error {
		env.SetDocument(d)
		return fn(&env)
	})
}

func (it *TableIterator) String() string {
	return fmt.Sprintf(".%s()", it.Table.Name())
}
