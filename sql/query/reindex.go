package query

import (
	"context"

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
func (stmt ReIndexStmt) Run(ctx context.Context, tx *database.Transaction, args []expr.Param) (Result, error) {
	var res Result

	if stmt.TableOrIndexName == "" {
		return res, tx.ReIndexAll(ctx)
	}

	t, err := tx.GetTable(ctx, stmt.TableOrIndexName)
	if err == nil {
		return res, t.ReIndex(ctx)
	}
	if err != database.ErrTableNotFound {
		return res, err
	}

	err = tx.ReIndex(ctx, stmt.TableOrIndexName)
	return res, err
}
