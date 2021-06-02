package statement

import (
	"errors"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/internal/database"
	"github.com/genjidb/genji/internal/expr"
)

// A Statement represents a unique action that can be executed against the database.
type Statement interface {
	Run(*database.Transaction, []expr.Param) (Result, error)
	IsReadOnly() bool
}

type Preparer interface {
	Prepare(tx *database.Transaction) error
}

// Result of a query.
type Result struct {
	Iterator document.Iterator
	Tx       *database.Transaction
	closed   bool
	err      error
}

func (r *Result) Iterate(fn func(d document.Document) error) error {
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
