package query

import (
	"database/sql/driver"
	"errors"

	"github.com/asdine/genji/database"
)

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
func (stmt DropIndexStmt) Run(tx *database.Transaction, args []driver.NamedValue) (Result, error) {
	var res Result

	if stmt.IndexName == "" {
		return res, errors.New("missing index name")
	}

	err := tx.DropIndex(stmt.IndexName)
	if err == database.ErrIndexNotFound && stmt.IfExists {
		err = nil
	}

	return res, err
}
