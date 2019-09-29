package query

import (
	"database/sql/driver"
	"errors"

	"github.com/asdine/genji/database"
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

// IsReadOnly always returns false. It implements the Statement interface.
func (stmt CreateTableStmt) IsReadOnly() bool {
	return false
}

// Run runs the Create table statement in the given transaction.
// It implements the Statement interface.
func (stmt CreateTableStmt) Run(tx *database.Tx, args []driver.NamedValue) Result {
	return stmt.exec(tx, args)
}

// Exec the CreateTable statement within tx.
func (stmt CreateTableStmt) Exec(tx *database.Tx, args ...interface{}) Result {
	return stmt.exec(tx, argsToNamedValues(args))
}

// IfNotExists sets the ifNotExists flag to true.
func (stmt CreateTableStmt) IfNotExists() CreateTableStmt {
	stmt.ifNotExists = true
	return stmt
}

func (stmt CreateTableStmt) exec(tx *database.Tx, _ []driver.NamedValue) Result {
	if stmt.tableName == "" {
		return Result{err: errors.New("missing table name")}
	}

	var err error
	if stmt.ifNotExists {
		_, err = tx.CreateTableIfNotExists(stmt.tableName)
	} else {
		_, err = tx.CreateTable(stmt.tableName)
	}

	return Result{err: err}
}
