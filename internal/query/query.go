package query

import (
	"context"
	"errors"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/internal/database"
	"github.com/genjidb/genji/internal/expr"
)

// A Query can execute statements against the database. It can read or write data
// from any table, or even alter the structure of the database.
// Results are returned as streams.
type Query struct {
	Statements []Statement
	tx         *database.Transaction
	autoCommit bool
}

// Run executes all the statements in their own transaction and returns the last result.
func (q Query) Run(ctx context.Context, db *database.Database, args []expr.Param) (*Result, error) {
	var res Result
	var err error

	q.tx = db.GetAttachedTx()
	if q.tx == nil {
		q.autoCommit = true
	}

	type queryAlterer interface {
		alterQuery(ctx context.Context, db *database.Database, q *Query) error
	}

	for i, stmt := range q.Statements {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		// reinitialize the result
		res = Result{}

		if qa, ok := stmt.(queryAlterer); ok {
			err = qa.alterQuery(ctx, db, &q)
			if err != nil {
				if tx := db.GetAttachedTx(); tx != nil {
					tx.Rollback()
				}
				return nil, err
			}

			continue
		}

		if q.tx == nil {
			q.tx, err = db.BeginTx(ctx, &database.TxOptions{
				ReadOnly: stmt.IsReadOnly(),
			})
			if err != nil {
				return nil, err
			}
		}

		res, err = stmt.Run(q.tx, args)
		if err != nil {
			if q.autoCommit {
				q.tx.Rollback()
			}

			return nil, err
		}

		// if there are still statements to be executed,
		// and the current statement is not read-only,
		// iterate over the result.
		if !stmt.IsReadOnly() && i+1 < len(q.Statements) {
			err = res.Iterate(func(d document.Document) error { return nil })
			if err != nil {
				return nil, err
			}
		}

		// it there is an opened transaction but there are still statements
		// to be executed, close the current transaction.
		if q.tx != nil && q.autoCommit && i+1 < len(q.Statements) {
			if q.tx.Writable {
				err := q.tx.Commit()
				if err != nil {
					return nil, err
				}
			} else {
				err := q.tx.Rollback()
				if err != nil {
					return nil, err
				}
			}
			q.tx = nil
		}
	}

	if q.autoCommit {
		// the returned result will now own the transaction.
		// its Close method is expected to be called.
		res.Tx = q.tx
	}

	return &res, nil
}

// Exec the query within the given transaction.
func (q Query) Exec(tx *database.Transaction, args []expr.Param) (*Result, error) {
	var res Result
	var err error

	for i, stmt := range q.Statements {
		res, err = stmt.Run(tx, args)
		if err != nil {
			return nil, err
		}

		// if there are still statements to be executed,
		// and the current statement is not read-only,
		// iterate over the result.
		if !stmt.IsReadOnly() && i+1 < len(q.Statements) {
			err = res.Iterate(func(d document.Document) error { return nil })
			if err != nil {
				return nil, err
			}
		}
	}

	return &res, nil
}

// New creates a new query with the given statements.
func New(statements ...Statement) Query {
	return Query{Statements: statements}
}

// A Statement represents a unique action that can be executed against the database.
type Statement interface {
	Run(*database.Transaction, []expr.Param) (Result, error)
	IsReadOnly() bool
}

// Result of a query.
type Result struct {
	Iterator document.Iterator
	Tx       *database.Transaction
	closed   bool
}

func (r *Result) Iterate(fn func(d document.Document) error) error {
	if r.Iterator == nil {
		return nil
	}

	return r.Iterator.Iterate(fn)
}

// Close the result stream.
// After closing the result, Stream is not supposed to be used.
// If the result stream was already closed, it returns
// ErrResultClosed.
func (r *Result) Close() (err error) {
	if r == nil {
		return nil
	}

	if r.closed {
		return errors.New("result already closed")
	}

	r.closed = true

	if r.Tx != nil {
		if r.Tx.Writable {
			err = r.Tx.Commit()
		} else {
			err = r.Tx.Rollback()
		}
	}

	return err
}
