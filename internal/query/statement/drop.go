package statement

import (
	"errors"

	errs "github.com/genjidb/genji/errors"
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
func (stmt DropTableStmt) Run(ctx *Context) (Result, error) {
	var res Result

	if stmt.TableName == "" {
		return res, errors.New("missing table name")
	}

	tb, err := ctx.Catalog.GetTable(ctx.Tx, stmt.TableName)
	if err != nil {
		if errs.IsNotFoundError(err) && stmt.IfExists {
			err = nil
		}

		return res, err
	}

	err = ctx.Catalog.DropTable(ctx.Tx, stmt.TableName)
	if err != nil {
		return res, err
	}

	// if there is no primary key, drop the docid sequence
	if tb.Info.FieldConstraints.GetPrimaryKey() == nil {
		err = ctx.Catalog.DropSequence(ctx.Tx, tb.Info.DocidSequenceName)
		if err != nil {
			return res, err
		}
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
func (stmt DropIndexStmt) Run(ctx *Context) (Result, error) {
	var res Result

	if stmt.IndexName == "" {
		return res, errors.New("missing index name")
	}

	err := ctx.Catalog.DropIndex(ctx.Tx, stmt.IndexName)
	if errs.IsNotFoundError(err) && stmt.IfExists {
		err = nil
	}

	return res, err
}
