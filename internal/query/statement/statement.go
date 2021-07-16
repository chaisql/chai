package statement

import (
	"errors"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/internal/database"
	"github.com/genjidb/genji/internal/environment"
	"github.com/genjidb/genji/types"
)

// A Statement represents a unique action that can be executed against the database.
type Statement interface {
	Run(*Context) (Result, error)
	IsReadOnly() bool
}

type Context struct {
	Tx      *database.Transaction
	Catalog database.Catalog
	Params  []environment.Param
}

type Preparer interface {
	Prepare(tx *Context) error
}

// Result of a query.
type Result struct {
	Iterator document.Iterator
	Tx       *database.Transaction
	closed   bool
	err      error
}

func (r *Result) Iterate(fn func(d types.Document) error) error {
	if r.Iterator == nil {
		return nil
	}

	r.err = r.Iterator.Iterate(fn)
	return r.err
}

// Close the result stream.
// After closing the result, Stream is not supposed to be used.
// If the result stream was already closed, it returns an error.
func (r *Result) Close() (err error) {
	if r == nil {
		return nil
	}

	if r.closed {
		return errors.New("result already closed")
	}

	r.closed = true

	if r.Tx != nil {
		if r.Tx.Writable && r.err == nil {
			err = r.Tx.Commit()
		} else {
			err = r.Tx.Rollback()
		}
	}

	return err
}
