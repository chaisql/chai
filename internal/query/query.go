package query

import (
	"context"

	"github.com/chaisql/chai/internal/database"
	"github.com/chaisql/chai/internal/environment"
	"github.com/chaisql/chai/internal/query/statement"
)

// A Query can execute statements against the database. It can read or write data
// from any table, or even alter the structure of the database.
// Results are returned as streams.
type Query struct {
	Statements []statement.Statement
}

// New creates a new query with the given statements.
func New(statements ...statement.Statement) *Query {
	return &Query{Statements: statements}
}

type Context struct {
	Ctx    context.Context
	DB     *database.Database
	Conn   *database.Connection
	Params []environment.Param
}

// Run executes all the statements in their own transaction and returns the last result.
func (q *Query) Run(c *Context) (*statement.Result, error) {
	var res *statement.Result
	var err error

	ctx := c.Ctx
	if ctx == nil {
		ctx = context.Background()
	}

	sctx := statement.Context{
		DB:     c.DB,
		Conn:   c.Conn,
		Params: c.Params,
	}

	var tx *database.Transaction

	for i, stmt := range q.Statements {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		// reinitialize the result
		res = nil

		// handles transactions
		isReadOnly := false
		if ro, ok := stmt.(statement.ReadOnly); ok {
			isReadOnly = ro.IsReadOnly()
		}

		needsTx := true
		if ntx, ok := stmt.(statement.Transactional); ok {
			needsTx = ntx.NeedsTransaction()
		}

		var autoCommit bool

		if c.Conn.GetTx() == nil && needsTx {
			autoCommit = true

			tx, err = c.Conn.BeginTx(&database.TxOptions{
				ReadOnly: isReadOnly,
			})
			if err != nil {
				return nil, err
			}
		}

		// prepare
		stmt, err = Prepare(&sctx, stmt)
		if err != nil {
			if tx != nil {
				_ = tx.Rollback()
			}

			return nil, err
		}

		// run
		res, err = stmt.Run(&sctx)
		if err != nil {
			if tx != nil {
				_ = tx.Rollback()
			}

			return nil, err
		}
		if res == nil {
			res = &statement.Result{}
		}

		// if there are still statements to be executed,
		// and the current statement is not read-only,
		// iterate over the result.
		if res != nil && !isReadOnly && i+1 < len(q.Statements) {
			err = res.Skip(ctx)
			if err != nil {
				if tx != nil {
					_ = tx.Rollback()
				}

				return nil, err
			}
		}

		// it there is an opened transaction but there are still statements
		// to be executed, close the current transaction.
		if autoCommit && i+1 < len(q.Statements) {
			if tx.Writable {
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
			tx = nil
		}
	}

	if tx != nil {
		// the returned result will now own the transaction.
		// its Close method is expected to be called.
		res.Tx = tx
	}

	return res, nil
}

// Prepare a statement by binding and preparing it.
// If there is no ongoing transaction, a read-only transaction
// is created for the duration of the preparation.
func Prepare(ctx *statement.Context, stmt statement.Statement) (statement.Statement, error) {
	var err error

	if _, ok := stmt.(statement.Preparer); !ok {
		return stmt, nil
	}

	if _, ok := stmt.(PreparedStatement); ok {
		return stmt, nil
	}

	if ctx.Conn.GetTx() == nil {
		var tx *database.Transaction
		tx, err = ctx.Conn.BeginTx(&database.TxOptions{
			ReadOnly: true,
		})
		if err != nil {
			return nil, err
		}
		defer func() { _ = tx.Rollback() }()
	}

	// bind
	if b, ok := stmt.(statement.Bindable); ok {
		err = b.Bind(ctx)
		if err != nil {
			return nil, err
		}
	}

	// prepare
	if prep, ok := stmt.(statement.Preparer); ok {
		stmt, err = prep.Prepare(ctx)
		if err != nil {
			return nil, err
		}
	}

	return PreparedStatement{stmt}, nil
}

type PreparedStatement struct {
	statement.Statement
}
