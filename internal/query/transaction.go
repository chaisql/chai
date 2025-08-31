package query

import (
	"github.com/chaisql/chai/internal/database"
	"github.com/chaisql/chai/internal/query/statement"
	"github.com/cockroachdb/errors"
)

// BeginStmt is a statement that creates a new transaction.
type BeginStmt struct {
	Writable bool
}

func (stmt BeginStmt) NeedsTransaction() bool {
	return false
}

func (stmt BeginStmt) Run(ctx *statement.Context) (*statement.Result, error) {
	if ctx.Conn.GetTx() != nil {
		return nil, errors.New("cannot begin a transaction within a transaction")
	}

	_, err := ctx.Conn.BeginTx(&database.TxOptions{
		ReadOnly: !stmt.Writable,
	})
	return nil, err
}

// RollbackStmt is a statement that rollbacks the current active transaction.
type RollbackStmt struct{}

func (stmt RollbackStmt) NeedsTransaction() bool {
	return false
}

func (stmt RollbackStmt) Run(ctx *statement.Context) (*statement.Result, error) {
	tx := ctx.Conn.GetTx()
	if tx == nil {
		return nil, errors.New("cannot rollback with no active transaction")
	}

	return nil, tx.Rollback()
}

// CommitStmt is a statement that commits the current active transaction.
type CommitStmt struct{}

func (stmt CommitStmt) IsReadOnly() bool {
	return false
}

func (stmt CommitStmt) NeedsTransaction() bool {
	return false
}

func (stmt CommitStmt) Run(ctx *statement.Context) (*statement.Result, error) {
	tx := ctx.Conn.GetTx()
	if tx == nil {
		return nil, errors.New("cannot commit with no active transaction")
	}

	return nil, tx.Commit()
}
