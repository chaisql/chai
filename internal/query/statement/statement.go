package statement

import (
	"github.com/chaisql/chai/internal/database"
	"github.com/chaisql/chai/internal/environment"
	"github.com/cockroachdb/errors"
)

// A Statement represents a unique action that can be executed against the database.
type Statement interface {
	Run(*Context) (Result, error)
	IsReadOnly() bool
}

type basePreparedStatement struct {
	Preparer Preparer
	ReadOnly bool
}

func (stmt *basePreparedStatement) IsReadOnly() bool {
	return stmt.ReadOnly
}

func (stmt *basePreparedStatement) Run(ctx *Context) (Result, error) {
	s, err := stmt.Preparer.Prepare(ctx)
	if err != nil {
		return Result{}, err
	}

	return s.Run(ctx)
}

type Context struct {
	DB     *database.Database
	Tx     *database.Transaction
	Params []environment.Param
}

type Preparer interface {
	Prepare(*Context) (Statement, error)
}

// Result of a query.
type Result struct {
	Iterator database.RowIterator
	Tx       *database.Transaction
	closed   bool
	err      error
}

func (r *Result) Iterate(fn func(database.Row) error) error {
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
