package query

import (
	"database/sql/driver"
	"errors"

	"github.com/asdine/genji/database"
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
func (stmt DropTableStmt) Run(tx *database.Transaction, args []driver.NamedValue) (Result, error) {
	var res Result

	if stmt.TableName == "" {
		return res, errors.New("missing table name")
	}

	err := tx.DropTable(stmt.TableName)
	if err == database.ErrTableNotFound && stmt.IfExists {
		err = nil
	}

	return res, err
}
