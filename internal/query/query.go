package query

import (
	"context"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/internal/database"
	"github.com/genjidb/genji/internal/expr"
	"github.com/genjidb/genji/internal/query/statement"
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

// Run executes all the statements in their own transaction and returns the last result.
func (q Query) Run(ctx context.Context, db *database.Database, args []expr.Param) (*statement.Result, error) {
	var res statement.Result
	var err error

	q.tx = db.GetAttachedTx()
	if q.tx == nil {
		q.autoCommit = true
	}

	for i, stmt := range q.Statements {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		// reinitialize the result
		res = statement.Result{}

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
				if q.autoCommit {
					q.tx.Rollback()
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
	alterQuery(ctx context.Context, db *database.Database, q *Query) error
}

// Prepare the statements by calling their Prepare methods.
// It stops at the first statement that doesn't implement the statement.Preparer interface.
func (q Query) Prepare(ctx context.Context, db *database.Database) error {
	var err error
	var tx *database.Transaction

	for _, stmt := range q.Statements {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		p, ok := stmt.(statement.Preparer)
		if !ok {
			break
		}

		if tx == nil {
			tx = db.GetAttachedTx()
			if tx == nil {
				tx, err = db.BeginTx(ctx, &database.TxOptions{
					ReadOnly: true,
				})
				if err != nil {
					return err
				}
				defer tx.Rollback()
			}
		}

		err = p.Prepare(tx)
		if err != nil {
			return err
		}
	}

	return nil
}

func (q Query) PrepareTx(tx *database.Transaction) error {
	for _, stmt := range q.Statements {
		p, ok := stmt.(statement.Preparer)
		if !ok {
			break
		}

		err := p.Prepare(tx)
		if err != nil {
			return err
		}
	}

	return nil
}

// Exec the query within the given transaction.
func (q Query) Exec(tx *database.Transaction, args []expr.Param) (*statement.Result, error) {
	var res statement.Result
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
