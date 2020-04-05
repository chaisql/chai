package query

import (
	"errors"

	"github.com/asdine/genji/database"
	"github.com/asdine/genji/document"
)

// CreateTableStmt is a DSL that allows creating a full CREATE TABLE statement.
type CreateTableStmt struct {
	TableName   string
	IfNotExists bool
	Config      database.TableConfig
}

// IsReadOnly always returns false. It implements the Statement interface.
func (stmt CreateTableStmt) IsReadOnly() bool {
	return false
}

// Run runs the Create table statement in the given transaction.
// It implements the Statement interface.
func (stmt CreateTableStmt) Run(tx *database.Transaction, args []Param) (Result, error) {
	var res Result

	if stmt.TableName == "" {
		return res, errors.New("missing table name")
	}

	err := tx.CreateTable(stmt.TableName, &stmt.Config)
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
	Path        document.ValuePath
	IfNotExists bool
	Unique      bool
}

// IsReadOnly always returns false. It implements the Statement interface.
func (stmt CreateIndexStmt) IsReadOnly() bool {
	return false
}

// Run runs the Create index statement in the given transaction.
// It implements the Statement interface.
func (stmt CreateIndexStmt) Run(tx *database.Transaction, args []Param) (Result, error) {
	var res Result

	if stmt.TableName == "" {
		return res, errors.New("missing table name")
	}

	if stmt.IndexName == "" {
		return res, errors.New("missing index name")
	}

	if len(stmt.Path) == 0 {
		return res, errors.New("missing path")
	}

	err := tx.CreateIndex(database.IndexConfig{
		Unique:    stmt.Unique,
		IndexName: stmt.IndexName,
		TableName: stmt.TableName,
		Path:      stmt.Path,
	})
	if stmt.IfNotExists && err == database.ErrIndexAlreadyExists {
		err = nil
	}

	return res, err
}
