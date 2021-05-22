package query

import (
	"github.com/genjidb/genji/document"
	errs "github.com/genjidb/genji/errors"
	"github.com/genjidb/genji/internal/database"
	"github.com/genjidb/genji/internal/expr"
)

// CreateTableStmt is a DSL that allows creating a full CREATE TABLE statement.
type CreateTableStmt struct {
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

	err := tx.Catalog.CreateTable(tx, stmt.Info.TableName, &stmt.Info)
	if stmt.IfNotExists && err == errs.ErrTableAlreadyExists {
		return res, nil
	}

	for _, fc := range stmt.Info.FieldConstraints {
		if fc.IsUnique {
			err = tx.Catalog.CreateIndex(tx, &database.IndexInfo{
				TableName: stmt.Info.TableName,
				Paths:     []document.Path{fc.Path},
				Unique:    true,
				Types:     []document.ValueType{fc.Type},
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
	IfNotExists bool
	Info        database.IndexInfo
}

// IsReadOnly always returns false. It implements the Statement interface.
func (stmt CreateIndexStmt) IsReadOnly() bool {
	return false
}

// Run runs the Create index statement in the given transaction.
// It implements the Statement interface.
func (stmt CreateIndexStmt) Run(tx *database.Transaction, args []expr.Param) (Result, error) {
	var res Result

	err := tx.Catalog.CreateIndex(tx, &stmt.Info)
	if stmt.IfNotExists && err == errs.ErrIndexAlreadyExists {
		return res, nil
	}

	err = tx.Catalog.ReIndex(tx, stmt.Info.IndexName)
	return res, err
}
