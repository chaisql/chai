package query

import (
	"errors"

	"github.com/genjidb/genji/database"
	"github.com/genjidb/genji/sql/query/expr"
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
		return res, database.ErrTableAlreadyExists
	}

	err := tx.RenameTable(stmt.TableName, stmt.NewTableName)
	return res, err
}
