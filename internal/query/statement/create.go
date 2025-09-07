package statement

import (
	"github.com/chaisql/chai/internal/database"
	errs "github.com/chaisql/chai/internal/errors"
	"github.com/chaisql/chai/internal/planner"
	"github.com/chaisql/chai/internal/stream"
	"github.com/chaisql/chai/internal/stream/index"
	"github.com/chaisql/chai/internal/stream/table"
	"github.com/cockroachdb/errors"
)

var _ Statement = (*CreateTableStmt)(nil)
var _ Statement = (*CreateIndexStmt)(nil)
var _ Statement = (*CreateSequenceStmt)(nil)

// CreateTableStmt represents a parsed CREATE TABLE statement.
type CreateTableStmt struct {
	IfNotExists bool
	Info        database.TableInfo
}

// Run runs the Create table statement in the given transaction.
// It implements the Statement interface.
func (stmt *CreateTableStmt) Run(ctx *Context) (*Result, error) {
	if stmt.Info.PrimaryKey == nil {
		return nil, errors.New("table must have a primary key")
	}

	// for each primary key column, add a not null constraint
	for _, col := range stmt.Info.PrimaryKey.Columns {
		cc := stmt.Info.GetColumnConstraint(col)
		if cc == nil {
			return nil, errors.Errorf("primary key column %q does not exist", col)
		}
		cc.IsNotNull = true
	}

	err := ctx.Conn.GetTx().CatalogWriter().CreateTable(ctx.Conn.GetTx(), stmt.Info.TableName, &stmt.Info)
	if stmt.IfNotExists {
		if errors.Is(err, errs.AlreadyExistsError{}) {
			return nil, nil
		}
	}

	// create a unique index for every unique constraint
	for _, tc := range stmt.Info.TableConstraints {
		if tc.Unique {
			_, err = ctx.Conn.GetTx().CatalogWriter().CreateIndex(ctx.Conn.GetTx(), &database.IndexInfo{
				Columns: tc.Columns,
				Unique:  true,
				Owner: database.Owner{
					TableName: stmt.Info.TableName,
					Columns:   tc.Columns,
				},
				KeySortOrder: tc.SortOrder,
			})
			if err != nil {
				return nil, err
			}
		}
	}

	return nil, err
}

// CreateIndexStmt represents a parsed CREATE INDEX statement.
type CreateIndexStmt struct {
	IfNotExists bool
	Info        database.IndexInfo
}

// Run runs the Create index statement in the given transaction.
// It implements the Statement interface.
func (stmt *CreateIndexStmt) Run(ctx *Context) (*Result, error) {
	_, err := ctx.Conn.GetTx().CatalogWriter().CreateIndex(ctx.Conn.GetTx(), &stmt.Info)
	if stmt.IfNotExists {
		if errs.IsAlreadyExistsError(err) {
			return nil, nil
		}
	}
	if err != nil {
		return nil, err
	}

	s := stream.New(table.Scan(stmt.Info.Owner.TableName)).
		Pipe(index.Insert(stmt.Info.IndexName)).
		Pipe(stream.Discard())

	st, err := planner.Optimize(s, ctx.Conn.GetTx().Catalog, ctx.Params)
	if err != nil {
		return nil, err
	}

	return &Result{
		Result: &StreamStmtResult{
			Stream:  st,
			Context: ctx,
		},
	}, nil
}

// CreateSequenceStmt represents a parsed CREATE SEQUENCE statement.
type CreateSequenceStmt struct {
	IfNotExists bool
	Info        database.SequenceInfo
}

// Run the statement in the given transaction.
// It implements the Statement interface.
func (stmt *CreateSequenceStmt) Run(ctx *Context) (*Result, error) {
	err := ctx.Conn.GetTx().CatalogWriter().CreateSequence(ctx.Conn.GetTx(), &stmt.Info)
	if stmt.IfNotExists {
		if errs.IsAlreadyExistsError(err) {
			return nil, nil
		}
	}
	return nil, err
}
