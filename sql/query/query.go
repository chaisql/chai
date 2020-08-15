package query

import (
	"errors"

	"github.com/genjidb/genji/database"
	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/sql/query/expr"
)

// ErrResultClosed is returned when trying to close an already closed result.
var ErrResultClosed = errors.New("result already closed")

// A Query can execute statements against the database. It can read or write data
// from any table, or even alter the structure of the database.
// Results are returned as streams.
type Query struct {
	Statements []Statement
}

// Run executes all the statements in their own transaction and returns the last result.
func (q Query) Run(db *database.Database, args []expr.Param) (*Result, error) {
	var res Result
	var tx *database.Transaction
	var err error

	for _, stmt := range q.Statements {
		// it there is an opened transaction but there are still statements
		// to be executed, close the current transaction.
		if tx != nil {
			if tx.Writable() {
				err := tx.Commit()
				if err != nil {
					return nil, err
				}
			} else {
				err := tx.Rollback()
				if err != nil {
					return nil, err
				}
			}
		}

		// start a new transaction for every statement
		tx, err = db.Begin(!stmt.IsReadOnly())
		if err != nil {
			return nil, err
		}

		res, err = stmt.Run(tx, args)
		if err != nil {
			tx.Rollback()
			return nil, err
		}
	}

	// the returned result will now own the transaction.
	// its Close method is expected to be called.
	res.Tx = tx

	return &res, nil
}

// Exec the query within the given transaction.
func (q Query) Exec(tx *database.Transaction, args []expr.Param) (*Result, error) {
	var res Result
	var err error

	for _, stmt := range q.Statements {
		res, err = stmt.Run(tx, args)
		if err != nil {
			return nil, err
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
	document.Stream
	RowsAffected  int64
	LastInsertKey []byte
	Tx            *database.Transaction
	closed        bool
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
		return ErrResultClosed
	}

	r.closed = true

	if r.Tx != nil {
		if r.Tx.Writable() {
			err = r.Tx.Commit()
		} else {
			err = r.Tx.Rollback()
		}
	}

	return err
}

func whereClause(e expr.Expr, stack expr.EvalStack) func(d document.Document) (bool, error) {
	if e == nil {
		return func(d document.Document) (bool, error) {
			return true, nil
		}
	}

	return func(d document.Document) (bool, error) {
		stack.Document = d
		v, err := e.Eval(stack)
		if err != nil {
			return false, err
		}

		return v.IsTruthy()
	}
}
