package query

import (
	"errors"

	errs "github.com/genjidb/genji/errors"
	"github.com/genjidb/genji/internal/database"
	"github.com/genjidb/genji/internal/expr"
)

// DropTableStmt is a DSL that allows creating a DROP TABLE query.
type DropTableStmt struct {
	TableName string
	IfExists  bool
}

// IsReadOnly always returns false. It implements the Statement interface.
func (stmt DropTableStmt) IsReadOnly() bool {
	return false
}

// Run runs the DropTable statement in the given transaction.
// It implements the Statement interface.
func (stmt DropTableStmt) Run(tx *database.Transaction, args []expr.Param) (Result, error) {
	var res Result

	if stmt.TableName == "" {
		return res, errors.New("missing table name")
	}

	err := tx.Catalog.DropTable(tx, stmt.TableName)
	if errors.Is(err, errs.ErrTableNotFound) && stmt.IfExists {
		err = nil
	}

	return res, err
}

// DropIndexStmt is a DSL that allows creating a DROP INDEX query.
type DropIndexStmt struct {
	IndexName string
	IfExists  bool
}

// IsReadOnly always returns false. It implements the Statement interface.
func (stmt DropIndexStmt) IsReadOnly() bool {
	return false
}

// Run runs the DropIndex statement in the given transaction.
// It implements the Statement interface.
func (stmt DropIndexStmt) Run(tx *database.Transaction, args []expr.Param) (Result, error) {
	var res Result

	if stmt.IndexName == "" {
		return res, errors.New("missing index name")
	}

	err := tx.Catalog.DropIndex(tx, stmt.IndexName)
	if err == errs.ErrIndexNotFound && stmt.IfExists {
		err = nil
	}

	return res, err
}
