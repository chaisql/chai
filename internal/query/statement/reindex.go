package statement

import (
	errs "github.com/genjidb/genji/errors"
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
func (stmt ReIndexStmt) Run(ctx *Context) (Result, error) {
	var res Result

	if stmt.TableOrIndexName == "" {
		return res, ctx.Catalog.ReIndexAll(ctx.Tx)
	}

	_, err := ctx.Catalog.GetTable(ctx.Tx, stmt.TableOrIndexName)
	if err == nil {
		for _, idxName := range ctx.Catalog.ListIndexes(stmt.TableOrIndexName) {
			err = ctx.Catalog.ReIndex(ctx.Tx, idxName)
			if err != nil {
				return res, err
			}
		}

		return res, nil
	}
	if !errs.IsNotFoundError(err) {
		return res, err
	}

	err = ctx.Catalog.ReIndex(ctx.Tx, stmt.TableOrIndexName)
	return res, err
}
