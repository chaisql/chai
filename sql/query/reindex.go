package query

import (
	"github.com/genjidb/genji/database"
	"github.com/genjidb/genji/sql/query/expr"
)

// ReIndexStmt is a DSL that allows creating a full REINDEX statement.
type ReIndexStmt struct {
	TableOrIndexName string
}

// IsReadOnly always returns false. It implements the Statement interface.
func (stmt ReIndexStmt) IsReadOnly() bool {
	return false
}

// Run runs the Reindex statement in the given transaction.
// It implements the Statement interface.
func (stmt ReIndexStmt) Run(tx *database.Transaction, args []expr.Param) (Result, error) {
	var res Result

	if stmt.TableOrIndexName == "" {
		return res, tx.ReIndexAll()
	}

	t, err := tx.GetTable(stmt.TableOrIndexName)
	if err == nil {
		return res, t.ReIndex()
	}
	if err != database.ErrTableNotFound {
		return res, err
	}

	err = tx.ReIndex(stmt.TableOrIndexName)
	return res, err
}
