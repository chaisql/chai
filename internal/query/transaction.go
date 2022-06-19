package query

import (
	"github.com/cockroachdb/errors"
	"github.com/genjidb/genji/internal/database"
	"github.com/genjidb/genji/internal/query/statement"
)

// BeginStmt is a statement that creates a new transaction.
type BeginStmt struct {
	Writable bool
}

// Prepare implements the Preparer interface.
func (stmt BeginStmt) Prepare(*statement.Context) (statement.Statement, error) {
	return stmt, nil
}

func (stmt BeginStmt) alterQuery(db *database.Database, q *Query) error {
	if q.tx != nil {
		return errors.New("cannot begin a transaction within a transaction")
	}

	var err error
	q.tx, err = db.BeginTx(&database.TxOptions{
		ReadOnly: !stmt.Writable,
		Attached: true,
	})
	q.autoCommit = false
	return err
}

func (stmt BeginStmt) IsReadOnly() bool {
	return !stmt.Writable
}

func (stmt BeginStmt) Run(ctx *statement.Context) (statement.Result, error) {
	return statement.Result{}, errors.New("cannot begin a transaction within a transaction")
}

// RollbackStmt is a statement that rollbacks the current active transaction.
type RollbackStmt struct{}

// Prepare implements the Preparer interface.
func (stmt RollbackStmt) Prepare(*statement.Context) (statement.Statement, error) {
	return stmt, nil
}

func (stmt RollbackStmt) alterQuery(db *database.Database, q *Query) error {
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

func (stmt RollbackStmt) Run(ctx *statement.Context) (statement.Result, error) {
	return statement.Result{}, errors.New("cannot rollback with no active transaction")
}

// CommitStmt is a statement that commits the current active transaction.
type CommitStmt struct{}

// Prepare implements the Preparer interface.
func (stmt CommitStmt) Prepare(*statement.Context) (statement.Statement, error) {
	return stmt, nil
}

func (stmt CommitStmt) alterQuery(db *database.Database, q *Query) error {
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

func (stmt CommitStmt) Run(ctx *statement.Context) (statement.Result, error) {
	return statement.Result{}, errors.New("cannot commit with no active transaction")
}
