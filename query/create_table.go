package query

import (
	"database/sql/driver"
	"errors"

	"github.com/asdine/genji/database"
	"github.com/asdine/genji/value"
)

// createTableStmt is a DSL that allows creating a full CREATE TABLE statement.
type createTableStmt struct {
	tableName      string
	ifNotExists    bool
	primaryKeyName string
	primaryKeyType value.Type
}

// IsReadOnly always returns false. It implements the Statement interface.
func (stmt createTableStmt) IsReadOnly() bool {
	return false
}

// Run runs the Create table statement in the given transaction.
// It implements the Statement interface.
func (stmt createTableStmt) Run(tx *database.Transaction, args []driver.NamedValue) (Result, error) {
	var res Result

	if stmt.tableName == "" {
		return res, errors.New("missing table name")
	}

	var cfg *database.TableConfig

	if stmt.primaryKeyName != "" {
		cfg = new(database.TableConfig)
		cfg.PrimaryKeyName = stmt.primaryKeyName
		cfg.PrimaryKeyType = stmt.primaryKeyType
	}

	err := tx.CreateTable(stmt.tableName, cfg)
	if stmt.ifNotExists && err == database.ErrTableAlreadyExists {
		err = nil
	}

	return res, err
}
