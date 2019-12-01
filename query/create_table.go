package query

import (
	"database/sql/driver"
	"errors"

	"github.com/asdine/genji/database"
	"github.com/asdine/genji/document"
)

// CreateTableStmt is a DSL that allows creating a full CREATE TABLE statement.
type CreateTableStmt struct {
	TableName      string
	IfNotExists    bool
	PrimaryKeyName string
	PrimaryKeyType document.ValueType
}

// IsReadOnly always returns false. It implements the Statement interface.
func (stmt CreateTableStmt) IsReadOnly() bool {
	return false
}

// Run runs the Create table statement in the given transaction.
// It implements the Statement interface.
func (stmt CreateTableStmt) Run(tx *database.Transaction, args []driver.NamedValue) (Result, error) {
	var res Result

	if stmt.TableName == "" {
		return res, errors.New("missing table name")
	}

	var cfg *database.TableConfig

	if stmt.PrimaryKeyName != "" {
		cfg = new(database.TableConfig)
		cfg.PrimaryKeyName = stmt.PrimaryKeyName
		cfg.PrimaryKeyType = stmt.PrimaryKeyType
	}

	err := tx.CreateTable(stmt.TableName, cfg)
	if stmt.IfNotExists && err == database.ErrTableAlreadyExists {
		err = nil
	}

	return res, err
}
