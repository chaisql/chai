package query

import (
	"errors"

	"github.com/asdine/genji"
)

// CreateTableStmt is a DSL that allows creating a full CREATE TABLE statement.
// It is typically created using the CreateTable function.
type CreateTableStmt struct {
	tableName   string
	ifNotExists bool
}

// CreateTable creates a DSL equivalent to the SQL CREATE TABLE command.
func CreateTable(tableName string) CreateTableStmt {
	return CreateTableStmt{tableName: tableName}
}

// Run the Create statement in a read-write transaction.
// It implements the Statement interface.
func (c CreateTableStmt) Run(txm *TxOpener) (res Result) {
	err := txm.Update(func(tx *genji.Tx) error {
		res = c.Exec(tx)
		return nil
	})

	if res.err != nil {
		return
	}

	if err != nil {
		res.err = err
	}

	return
}

// IfNotExists sets the ifNotExists flag to true.
func (c CreateTableStmt) IfNotExists() CreateTableStmt {
	c.ifNotExists = true
	return c
}

// Exec the CreateTable statement within tx.
func (c CreateTableStmt) Exec(tx *genji.Tx) Result {
	if c.tableName == "" {
		return Result{err: errors.New("missing table name")}
	}

	var err error
	if c.ifNotExists {
		_, err = tx.CreateTableIfNotExists(c.tableName)
	} else {
		_, err = tx.CreateTable(c.tableName)
	}

	return Result{err: err}
}
