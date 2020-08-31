package query

import (
	"errors"

	"github.com/genjidb/genji/database"
	"github.com/genjidb/genji/sql/query/expr"
)

// BeginStmt is a statement that creates a new transaction.
type BeginStmt struct {
	Writable bool
}

func (stmt BeginStmt) alterQuery(db *database.Database, q *Query) error {
	if q.tx != nil {
		return errors.New("cannot begin a transaction within a transaction")
	}

	var err error
	q.tx, err = db.Begin(stmt.Writable)
	if err != nil {
		return err
	}
	return db.SetActiveTransaction(q.tx)
}

func (stmt BeginStmt) IsReadOnly() bool {
	return !stmt.Writable
}

func (stmt BeginStmt) Run(tx *database.Transaction, args []expr.Param) (Result, error) {
	return Result{}, nil
}

// RollbackStmt is a statement that rollbacks the current active transaction.
type RollbackStmt struct{}

func (stmt RollbackStmt) alterQuery(db *database.Database, q *Query) error {
	if q.tx == nil || q.autoCommit == true {
		return errors.New("cannot rollback with no active transaction")
	}

	return q.tx.Rollback()
}

func (stmt RollbackStmt) IsReadOnly() bool {
	return false
}

func (stmt RollbackStmt) Run(tx *database.Transaction, args []expr.Param) (Result, error) {
	return Result{}, nil
}

// CommitStmt is a statement that commits the current active transaction.
type CommitStmt struct{}

func (stmt CommitStmt) alterQuery(db *database.Database, q *Query) error {
	if q.tx == nil || q.autoCommit == true {
		return errors.New("cannot rollback with no active transaction")
	}

	return q.tx.Commit()
}

func (stmt CommitStmt) IsReadOnly() bool {
	return false
}

func (stmt CommitStmt) Run(tx *database.Transaction, args []expr.Param) (Result, error) {
	return Result{}, nil
}
