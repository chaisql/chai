package query

import (
	"github.com/genjidb/genji/database"
	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/expr"
)

// CreateTableStmt is a DSL that allows creating a full CREATE TABLE statement.
type CreateTableStmt struct {
	TableName   string
	IfNotExists bool
	Info        database.TableInfo
}

// IsReadOnly always returns false. It implements the Statement interface.
func (stmt CreateTableStmt) IsReadOnly() bool {
	return false
}

// Run runs the Create table statement in the given transaction.
// It implements the Statement interface.
func (stmt CreateTableStmt) Run(tx *database.Transaction, args []expr.Param) (Result, error) {
	var res Result

	err := tx.CreateTable(stmt.TableName, &stmt.Info)
	if stmt.IfNotExists && err == database.ErrTableAlreadyExists {
		err = nil
	}

	for _, fc := range stmt.Info.FieldConstraints {
		if fc.IsUnique {
			err = tx.CreateIndex(&database.IndexInfo{
				TableName: stmt.TableName,
				Path:      fc.Path,
				Unique:    true,
				Type:      fc.Type,
			})
			if err != nil {
				return res, err
			}
		}
	}

	return res, err
}

// CreateIndexStmt is a DSL that allows creating a full CREATE INDEX statement.
// It is typically created using the CreateIndex function.
type CreateIndexStmt struct {
	IndexName   string
	TableName   string
	Path        document.Path
	IfNotExists bool
	Unique      bool
}

// IsReadOnly always returns false. It implements the Statement interface.
func (stmt CreateIndexStmt) IsReadOnly() bool {
	return false
}

// Run runs the Create index statement in the given transaction.
// It implements the Statement interface.
func (stmt CreateIndexStmt) Run(tx *database.Transaction, args []expr.Param) (Result, error) {
	var res Result

	err := tx.CreateIndex(&database.IndexInfo{
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
