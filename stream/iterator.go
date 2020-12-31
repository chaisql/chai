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
