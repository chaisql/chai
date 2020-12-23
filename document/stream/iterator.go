package stream

import (
	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/sql/query/expr"
)

// An Iterator can iterate over values.
type Iterator interface {
	// Iterate goes through all the values and calls the given function by passing each one of them.
	// If the given function returns an error, the iteration stops.
	Iterate(fn func(env *expr.Environment) error) error
}

type valueIterator []document.Value

func (it valueIterator) Iterate(fn func(env *expr.Environment) error) error {
	var env expr.Environment

	for _, v := range it {
		env.SetCurrentValue(v)
		err := fn(&env)
		if err != nil {
			return err
		}
	}

	return nil
}

// NewValueIterator creates an iterator that iterates over the given values.
func NewValueIterator(values ...document.Value) Iterator {
	return valueIterator(values)
}

type arrayIterator struct {
	arr document.Array
}

func (it *arrayIterator) Iterate(fn func(env *expr.Environment) error) error {
	var env expr.Environment

	return it.arr.Iterate(func(i int, value document.Value) error {
		env.SetCurrentValue(value)
		return fn(&env)
	})
}

// NewArrayIterator creats an iterator that iterates over each values of the given array.
func NewArrayIterator(a document.Array) Iterator {
	return &arrayIterator{arr: a}
}
