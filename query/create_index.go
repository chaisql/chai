package query

import (
	"database/sql/driver"
	"errors"

	"github.com/asdine/genji/database"
	"github.com/asdine/genji/index"
)

// createIndexStmt is a DSL that allows creating a full CREATE INDEX statement.
// It is typically created using the CreateIndex function.
type createIndexStmt struct {
	indexName   string
	tableName   string
	fieldName   string
	ifNotExists bool
	unique      bool
}

// IsReadOnly always returns false. It implements the Statement interface.
func (stmt createIndexStmt) IsReadOnly() bool {
	return false
}

// Run runs the Create index statement in the given transaction.
// It implements the Statement interface.
func (stmt createIndexStmt) Run(tx *database.Transaction, args []driver.NamedValue) (Result, error) {
	var res Result

	if stmt.tableName == "" {
		return res, errors.New("missing table name")
	}

	if stmt.indexName == "" {
		return res, errors.New("missing index name")
	}

	if stmt.fieldName == "" {
		return res, errors.New("missing field name")
	}

	err := tx.CreateIndex(index.Options{
		Unique:    stmt.unique,
		IndexName: stmt.indexName,
		TableName: stmt.tableName,
		FieldName: stmt.fieldName,
	})
	if stmt.ifNotExists && err == database.ErrIndexAlreadyExists {
		err = nil
	}

	return res, err
}
