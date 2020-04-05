package query

import (
	"errors"

	"github.com/asdine/genji/database"
	"github.com/asdine/genji/document"
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
func (q Query) Run(db *database.Database, args []Param) (*Result, error) {
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
	res.tx = tx

	return &res, nil
}

// Exec the query within the given transaction. If the one of the statements requires a read-write
// transaction and tx is not, tx will get promoted.
func (q Query) Exec(tx *database.Transaction, args []Param, forceReadOnly bool) (*Result, error) {
	var res Result
	var err error

	for _, stmt := range q.Statements {
		// if the statement requires a writable transaction,
		// promote the current transaction.
		if !forceReadOnly && !tx.Writable() && !stmt.IsReadOnly() {
			err := tx.Promote()
			if err != nil {
				return nil, err
			}
		}

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
	Run(*database.Transaction, []Param) (Result, error)
	IsReadOnly() bool
}

// A Param represents a parameter passed by the user to the statement.
type Param struct {
	// Name of the param
	Name string

	// Value is the parameter value.
	Value interface{}
}

// Result of a query.
type Result struct {
	document.Stream
	RowsAffected  int64
	LastInsertKey []byte
	tx            *database.Transaction
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

	if r.tx != nil {
		if r.tx.Writable() {
			err = r.tx.Commit()
		} else {
			err = r.tx.Rollback()
		}
	}

	return err
}

func whereClause(e Expr, stack EvalStack) func(d document.Document) (bool, error) {
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

		return v.IsTruthy(), nil
	}
}
