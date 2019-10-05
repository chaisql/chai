package query

import (
	"database/sql/driver"
	"errors"

	"github.com/asdine/genji/database"
	"github.com/asdine/genji/index"
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

// CreateIndexStmt is a DSL that allows creating a full CREATE INDEX statement.
// It is typically created using the CreateIndex function.
type CreateIndexStmt struct {
	idxName     string
	tableName   string
	fieldName   string
	ifNotExists bool
	unique      bool
}

// CreateIndex creates a DSL equivalent to the SQL CREATE INDEX command.
func CreateIndex(idxName string) CreateIndexStmt {
	return CreateIndexStmt{
		idxName: idxName,
	}
}

// IsReadOnly always returns false. It implements the Statement interface.
func (stmt CreateIndexStmt) IsReadOnly() bool {
	return false
}

// Unique configures the statement to create a unique index.
func (stmt CreateIndexStmt) Unique() CreateIndexStmt {
	stmt.unique = true
	return stmt
}

// Run runs the Create table statement in the given transaction.
// It implements the Statement interface.
func (stmt CreateIndexStmt) Run(tx *database.Tx, args []driver.NamedValue) Result {
	return stmt.exec(tx, args)
}

// Exec the CreateTable statement within tx.
func (stmt CreateIndexStmt) Exec(tx *database.Tx, args ...interface{}) Result {
	return stmt.exec(tx, argsToNamedValues(args))
}

// IfNotExists sets the ifNotExists flag to true.
func (stmt CreateIndexStmt) IfNotExists() CreateIndexStmt {
	stmt.ifNotExists = true
	return stmt
}

// On selects the table.
func (stmt CreateIndexStmt) On(tableName string) CreateIndexStmt {
	stmt.tableName = tableName
	return stmt
}

// Field selects the field name.
func (stmt CreateIndexStmt) Field(fieldName string) CreateIndexStmt {
	stmt.fieldName = fieldName
	return stmt
}

func (stmt CreateIndexStmt) exec(tx *database.Tx, _ []driver.NamedValue) Result {
	if stmt.tableName == "" {
		return Result{err: errors.New("missing table name")}
	}

	if stmt.idxName == "" {
		return Result{err: errors.New("missing index name")}
	}

	if stmt.fieldName == "" {
		return Result{err: errors.New("missing field name")}
	}

	t, err := tx.GetTable(stmt.tableName)
	if err != nil {
		return Result{err: err}
	}

	if stmt.ifNotExists {
		_, err = t.CreateIndexIfNotExists(stmt.idxName, stmt.fieldName, index.Options{Unique: stmt.unique})
	} else {
		_, err = t.CreateIndex(stmt.idxName, stmt.fieldName, index.Options{Unique: stmt.unique})
	}

	return Result{err: err}
}
