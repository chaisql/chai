package query

import (
	"context"
	"errors"

	"github.com/genjidb/genji/internal/database"
	"github.com/genjidb/genji/internal/expr"
)

// BeginStmt is a statement that creates a new transaction.
type BeginStmt struct {
	Writable bool
}

func (stmt BeginStmt) alterQuery(ctx context.Context, db *database.Database, q *Query) error {
	if q.tx != nil {
		return errors.New("cannot begin a transaction within a transaction")
	}

	var err error
	q.tx, err = db.BeginTx(ctx, &database.TxOptions{
		ReadOnly: !stmt.Writable,
		Attached: true,
	})
	q.autoCommit = false
	return err
}

func (stmt BeginStmt) IsReadOnly() bool {
	return !stmt.Writable
}

func (stmt BeginStmt) Run(tx *database.Transaction, args []expr.Param) (Result, error) {
	return Result{}, errors.New("cannot begin a transaction within a transaction")
}

// RollbackStmt is a statement that rollbacks the current active transaction.
type RollbackStmt struct{}

func (stmt RollbackStmt) alterQuery(ctx context.Context, db *database.Database, q *Query) error {
	if q.tx == nil || q.autoCommit {
		return errors.New("cannot rollback with no active transaction")
	}

	err := q.tx.Rollback()
	if err != nil {
		return err
	}
	q.tx = nil
	q.autoCommit = true
	return nil
}

func (stmt RollbackStmt) IsReadOnly() bool {
	return false
}

func (stmt RollbackStmt) Run(tx *database.Transaction, args []expr.Param) (Result, error) {
	return Result{}, errors.New("cannot rollback with no active transaction")
}

// CommitStmt is a statement that commits the current active transaction.
type CommitStmt struct{}

func (stmt CommitStmt) alterQuery(ctx context.Context, db *database.Database, q *Query) error {
	if q.tx == nil || q.autoCommit {
		return errors.New("cannot commit with no active transaction")
	}

	err := q.tx.Commit()
	if err != nil {
		return err
	}
	q.tx = nil
	q.autoCommit = true
	return nil
}

func (stmt CommitStmt) IsReadOnly() bool {
	return false
}

func (stmt CommitStmt) Run(tx *database.Transaction, args []expr.Param) (Result, error) {
	return Result{}, errors.New("cannot commit with no active transaction")
}
