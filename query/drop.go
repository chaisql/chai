package query

import (
	"database/sql/driver"
	"errors"

	"github.com/asdine/genji/database"
)

// DropTableStmt is a DSL that allows creating a DROP TABLE query.
type DropTableStmt struct {
	tableName string
	ifExists  bool
}

// DropTable creates a DSL equivalent to the SQL DROP TABLE command.
func DropTable(tableName string) DropTableStmt {
	return DropTableStmt{
		tableName: tableName,
	}
}

// IsReadOnly always returns false. It implements the Statement interface.
func (stmt DropTableStmt) IsReadOnly() bool {
	return false
}

// Run runs the DropTable statement in the given transaction.
// It implements the Statement interface.
func (stmt DropTableStmt) Run(tx *database.Tx, args []driver.NamedValue) Result {
	return stmt.exec(tx, args)
}

// Exec the DropTable statement within tx.
func (stmt DropTableStmt) Exec(tx *database.Tx, args ...interface{}) Result {
	return stmt.exec(tx, argsToNamedValues(args))
}

// IfExists sets the ifExists flag to true.
func (stmt DropTableStmt) IfExists() DropTableStmt {
	stmt.ifExists = true
	return stmt
}

func (stmt DropTableStmt) exec(tx *database.Tx, args []driver.NamedValue) Result {
	if stmt.tableName == "" {
		return Result{err: errors.New("missing table name")}
	}

	err := tx.DropTable(stmt.tableName)
	if err == database.ErrTableNotFound && stmt.ifExists {
		return Result{}
	}

	return Result{err: err}
}

// DropIndexStmt is a DSL that allows creating a DROP INDEX query.
type DropIndexStmt struct {
	indexName string
	ifExists  bool
}

// DropIndex creates a DSL equivalent to the SQL DROP INDEX command.
func DropIndex(indexName string) DropIndexStmt {
	return DropIndexStmt{
		indexName: indexName,
	}
}

// IsReadOnly always returns false. It implements the Statement interface.
func (stmt DropIndexStmt) IsReadOnly() bool {
	return false
}

// Run runs the DropIndex statement in the given transaction.
// It implements the Statement interface.
func (stmt DropIndexStmt) Run(tx *database.Tx, args []driver.NamedValue) Result {
	return stmt.exec(tx, args)
}

// Exec the DropIndex statement within tx.
func (stmt DropIndexStmt) Exec(tx *database.Tx, args ...interface{}) Result {
	return stmt.exec(tx, argsToNamedValues(args))
}

// IfExists sets the ifExists flag to true.
func (stmt DropIndexStmt) IfExists() DropIndexStmt {
	stmt.ifExists = true
	return stmt
}

func (stmt DropIndexStmt) exec(tx *database.Tx, args []driver.NamedValue) Result {
	if stmt.indexName == "" {
		return Result{err: errors.New("missing index name")}
	}

	err := tx.DropIndex(stmt.indexName)
	if err == database.ErrIndexNotFound && stmt.ifExists {
		err = nil
	}

	return Result{err: err}
}
