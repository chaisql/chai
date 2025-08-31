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
	tx         *database.Transaction
	autoCommit bool
}

// New creates a new query with the given statements.
func New(statements ...statement.Statement) Query {
	return Query{Statements: statements}
}

type Context struct {
	Ctx    context.Context
	DB     *database.Database
	Conn   *database.Connection
	Params []environment.Param
}

func (c *Context) GetTx() *database.Transaction {
	return c.Conn.GetTx()
}

// Prepare the statements by calling their Prepare methods.
// It stops at the first statement that doesn't implement the statement.Preparer interface.
func (q *Query) Prepare(context *Context) error {
	var err error
	var tx *database.Transaction

	ctx := context.Ctx

	for i, stmt := range q.Statements {
		if ctx != nil {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
		}

		if tx == nil {
			tx = context.GetTx()
			if tx == nil {
				tx, err = context.Conn.BeginTx(&database.TxOptions{
					ReadOnly: true,
				})
				if err != nil {
					return err
				}
				defer tx.Rollback()
			}
		}

		sctx := &statement.Context{
			DB:   context.DB,
			Conn: context.Conn,
			Tx:   tx,
		}

		err = stmt.Bind(sctx)
		if err != nil {
			return err
		}

		p, ok := stmt.(statement.Preparer)
		if !ok {
			continue
		}

		stmt, err := p.Prepare(sctx)
		if err != nil {
			return err
		}

		q.Statements[i] = stmt
	}

	return nil
}

// Run executes all the statements in their own transaction and returns the last result.
func (q *Query) Run(context *Context) (*statement.Result, error) {
	var res statement.Result
	var err error

	q.tx = context.GetTx()
	if q.tx == nil {
		q.autoCommit = true
	}

	ctx := context.Ctx

	for i, stmt := range q.Statements {
		if ctx != nil {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			default:
			}
		}
		// reinitialize the result
		res = statement.Result{}

		if qa, ok := stmt.(queryAlterer); ok {
			err = qa.alterQuery(context.Conn, q)
			if err != nil {
				if tx := context.GetTx(); tx != nil {
					_ = tx.Rollback()
				}
				return nil, err
			}

			continue
		}

		if q.tx == nil {
			q.tx, err = context.Conn.BeginTx(&database.TxOptions{
				ReadOnly: stmt.IsReadOnly(),
			})
			if err != nil {
				return nil, err
			}
		}

		res, err = stmt.Run(&statement.Context{
			DB:     context.DB,
			Conn:   context.Conn,
			Tx:     q.tx,
			Params: context.Params,
		})
		if err != nil {
			if q.autoCommit {
				_ = q.tx.Rollback()
			}

			return nil, err
		}

		// if there are still statements to be executed,
		// and the current statement is not read-only,
		// iterate over the result.
		if !stmt.IsReadOnly() && i+1 < len(q.Statements) {
			err = res.Skip()
			if err != nil {
				if q.autoCommit {
					_ = q.tx.Rollback()
				}

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

type queryAlterer interface {
	alterQuery(conn *database.Connection, q *Query) error
}
