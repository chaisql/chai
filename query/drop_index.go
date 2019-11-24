package query

import (
	"database/sql/driver"
	"errors"

	"github.com/asdine/genji/database"
)

// dropIndexStmt is a DSL that allows creating a DROP INDEX query.
type dropIndexStmt struct {
	indexName string
	ifExists  bool
}

// IsReadOnly always returns false. It implements the Statement interface.
func (stmt dropIndexStmt) IsReadOnly() bool {
	return false
}

// Run runs the DropIndex statement in the given transaction.
// It implements the Statement interface.
func (stmt dropIndexStmt) Run(tx *database.Transaction, args []driver.NamedValue) (Result, error) {
	var res Result

	if stmt.indexName == "" {
		return res, errors.New("missing index name")
	}

	err := tx.DropIndex(stmt.indexName)
	if err == database.ErrIndexNotFound && stmt.ifExists {
		err = nil
	}

	return res, err
}
