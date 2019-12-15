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

// CreateIndexStmt is a DSL that allows creating a full CREATE INDEX statement.
// It is typically created using the CreateIndex function.
type CreateIndexStmt struct {
	IndexName   string
	TableName   string
	FieldName   string
	IfNotExists bool
	Unique      bool
}

// IsReadOnly always returns false. It implements the Statement interface.
func (stmt CreateIndexStmt) IsReadOnly() bool {
	return false
}

// Run runs the Create index statement in the given transaction.
// It implements the Statement interface.
func (stmt CreateIndexStmt) Run(tx *database.Transaction, args []driver.NamedValue) (Result, error) {
	var res Result

	if stmt.TableName == "" {
		return res, errors.New("missing table name")
	}

	if stmt.IndexName == "" {
		return res, errors.New("missing index name")
	}

	if stmt.FieldName == "" {
		return res, errors.New("missing field name")
	}

	err := tx.CreateIndex(database.IndexOptions{
		Unique:    stmt.Unique,
		IndexName: stmt.IndexName,
		TableName: stmt.TableName,
		FieldName: stmt.FieldName,
	})
	if stmt.IfNotExists && err == database.ErrIndexAlreadyExists {
		err = nil
	}

	return res, err
}
