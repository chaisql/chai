package statement

import (
	"fmt"

	errs "github.com/chaisql/chai/internal/errors"
	"github.com/cockroachdb/errors"
)

var _ Statement = (*DropTableStmt)(nil)
var _ Statement = (*DropIndexStmt)(nil)
var _ Statement = (*DropSequenceStmt)(nil)

// DropTableStmt is a DSL that allows creating a DROP TABLE query.
type DropTableStmt struct {
	TableName string
	IfExists  bool
}

// IsReadOnly always returns false. It implements the Statement interface.
func (stmt *DropTableStmt) IsReadOnly() bool {
	return false
}

func (stmt *DropTableStmt) Bind(ctx *Context) error {
	return nil
}

// Run runs the DropTable statement in the given transaction.
// It implements the Statement interface.
func (stmt *DropTableStmt) Run(ctx *Context) (*Result, error) {
	if stmt.TableName == "" {
		return nil, errors.New("missing table name")
	}

	tb, err := ctx.Conn.GetTx().Catalog.GetTable(ctx.Conn.GetTx(), stmt.TableName)
	if err != nil {
		if errs.IsNotFoundError(err) && stmt.IfExists {
			err = nil
		}

		return nil, err
	}

	err = ctx.Conn.GetTx().CatalogWriter().DropTable(ctx.Conn.GetTx(), stmt.TableName)
	if err != nil {
		return nil, err
	}

	// if there is no primary key, drop the rowid sequence
	if tb.Info.PrimaryKey == nil {
		err = ctx.Conn.GetTx().CatalogWriter().DropSequence(ctx.Conn.GetTx(), tb.Info.RowidSequenceName)
		if err != nil {
			return nil, err
		}
	}

	return nil, err
}

// DropIndexStmt is a DSL that allows creating a DROP INDEX query.
type DropIndexStmt struct {
	IndexName string
	IfExists  bool
}

// IsReadOnly always returns false. It implements the Statement interface.
func (stmt *DropIndexStmt) IsReadOnly() bool {
	return false
}

func (stmt *DropIndexStmt) Bind(ctx *Context) error {
	return nil
}

// Run runs the DropIndex statement in the given transaction.
// It implements the Statement interface.
func (stmt *DropIndexStmt) Run(ctx *Context) (*Result, error) {
	if stmt.IndexName == "" {
		return nil, errors.New("missing index name")
	}

	err := ctx.Conn.GetTx().CatalogWriter().DropIndex(ctx.Conn.GetTx(), stmt.IndexName)
	if errs.IsNotFoundError(err) && stmt.IfExists {
		err = nil
	}

	return nil, err
}

// DropSequenceStmt is a DSL that allows creating a DROP INDEX query.
type DropSequenceStmt struct {
	SequenceName string
	IfExists     bool
}

// IsReadOnly always returns false. It implements the Statement interface.
func (stmt *DropSequenceStmt) IsReadOnly() bool {
	return false
}

func (stmt *DropSequenceStmt) Bind(ctx *Context) error {
	return nil
}

// Run runs the DropSequence statement in the given transaction.
// It implements the Statement interface.
func (stmt *DropSequenceStmt) Run(ctx *Context) (*Result, error) {
	if stmt.SequenceName == "" {
		return nil, errors.New("missing index name")
	}

	seq, err := ctx.Conn.GetTx().Catalog.GetSequence(stmt.SequenceName)
	if err != nil {
		if errs.IsNotFoundError(err) && stmt.IfExists {
			err = nil
		}
		return nil, err
	}

	if seq.Info.Owner.TableName != "" {
		return nil, fmt.Errorf("cannot drop sequence %s because constraint of table %s requires it", seq.Info.Name, seq.Info.Owner.TableName)
	}

	err = ctx.Conn.GetTx().CatalogWriter().DropSequence(ctx.Conn.GetTx(), stmt.SequenceName)
	if errs.IsNotFoundError(err) && stmt.IfExists {
		err = nil
	}

	return nil, err
}
