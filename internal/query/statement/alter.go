package statement

import (
	"github.com/cockroachdb/errors"
	"github.com/genjidb/genji/internal/database"
	errs "github.com/genjidb/genji/internal/errors"
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
func (stmt AlterStmt) Run(ctx *Context) (Result, error) {
	var res Result

	if stmt.TableName == "" {
		return res, errors.New("missing table name")
	}

	if stmt.NewTableName == "" {
		return res, errors.New("missing new table name")
	}

	if stmt.TableName == stmt.NewTableName {
		return res, errs.AlreadyExistsError{Name: stmt.NewTableName}
	}

	err := ctx.Catalog.RenameTable(ctx.Tx, stmt.TableName, stmt.NewTableName)
	return res, err
}

type AlterTableAddField struct {
	Info database.TableInfo
}

// IsReadOnly always returns false. It implements the Statement interface.
func (stmt AlterTableAddField) IsReadOnly() bool {
	return false
}

// Run runs the ALTER TABLE ADD FIELD statement in the given transaction.
// It implements the Statement interface.
func (stmt AlterTableAddField) Run(ctx *Context) (Result, error) {
	var res Result

	var fc *database.FieldConstraint
	if len(stmt.Info.FieldConstraints.Ordered) != 0 {
		fc = stmt.Info.FieldConstraints.Ordered[0]
	}

	err := ctx.Catalog.AddFieldConstraint(ctx.Tx, stmt.Info.TableName, fc, stmt.Info.TableConstraints)
	return res, err
}
