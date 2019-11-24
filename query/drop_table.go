package query

import (
	"database/sql/driver"
	"errors"

	"github.com/asdine/genji/database"
)

// dropTableStmt is a DSL that allows creating a DROP TABLE query.
type dropTableStmt struct {
	tableName string
	ifExists  bool
}

// IsReadOnly always returns false. It implements the Statement interface.
func (stmt dropTableStmt) IsReadOnly() bool {
	return false
}

// Run runs the DropTable statement in the given transaction.
// It implements the Statement interface.
func (stmt dropTableStmt) Run(tx *database.Transaction, args []driver.NamedValue) (Result, error) {
	var res Result

	if stmt.tableName == "" {
		return res, errors.New("missing table name")
	}

	err := tx.DropTable(stmt.tableName)
	if err == database.ErrTableNotFound && stmt.ifExists {
		err = nil
	}

	return res, err
}
