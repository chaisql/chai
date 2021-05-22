package query

import (
	"errors"

	errs "github.com/genjidb/genji/errors"
	"github.com/genjidb/genji/internal/database"
	"github.com/genjidb/genji/internal/expr"
)

// AlterStmt is a DSL that allows creating a full ALTER TABLE query.
type AlterStmt struct {
	TableName    string
	NewTableName string
}

// IsReadOnly always returns false. It implements the Statement interface.
func (stmt AlterStmt) IsReadOnly() bool {
	return false
}

// Run runs the ALTER TABLE statement in the given transaction.
// It implements the Statement interface.
func (stmt AlterStmt) Run(tx *database.Transaction, _ []expr.Param) (Result, error) {
	var res Result

	if stmt.TableName == "" {
		return res, errors.New("missing table name")
	}

	if stmt.NewTableName == "" {
		return res, errors.New("missing new table name")
	}

	if stmt.TableName == stmt.NewTableName {
		return res, errs.ErrTableAlreadyExists
	}

	err := tx.Catalog.RenameTable(tx, stmt.TableName, stmt.NewTableName)
	return res, err
}

type AlterTableAddField struct {
	TableName  string
	Constraint database.FieldConstraint
}

// IsReadOnly always returns false. It implements the Statement interface.
func (stmt AlterTableAddField) IsReadOnly() bool {
	return false
}

// Run runs the ALTER TABLE ADD FIELD statement in the given transaction.
// It implements the Statement interface.
func (stmt AlterTableAddField) Run(tx *database.Transaction, _ []expr.Param) (Result, error) {
	var res Result

	if stmt.TableName == "" {
		return res, errors.New("missing table name")
	}

	if stmt.Constraint.Path == nil {
		return res, errors.New("missing field name")
	}

	err := tx.Catalog.AddFieldConstraint(tx, stmt.TableName, stmt.Constraint)
	return res, err
}
