package statement

import (
	"math"

	"github.com/chaisql/chai/internal/database"
	errs "github.com/chaisql/chai/internal/errors"
	"github.com/chaisql/chai/internal/stream"
	"github.com/chaisql/chai/internal/stream/index"
	"github.com/chaisql/chai/internal/stream/table"
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
	// if there is no primary key, create a rowid sequence
	if stmt.Info.PrimaryKey == nil {
		seq := database.SequenceInfo{
			IncrementBy: 1,
			Min:         1, Max: math.MaxInt64,
			Start: 1,
			Cache: 64,
			Owner: database.Owner{
				TableName: stmt.Info.TableName,
			},
		}
		err := ctx.Conn.GetTx().CatalogWriter().CreateSequence(ctx.Conn.GetTx(), &seq)
		if err != nil {
			return nil, err
		}

		stmt.Info.RowidSequenceName = seq.Name
	}

	err := ctx.Conn.GetTx().CatalogWriter().CreateTable(ctx.Conn.GetTx(), stmt.Info.TableName, &stmt.Info)
	if stmt.IfNotExists {
		if errs.IsAlreadyExistsError(err) {
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

	ss := PreparedStreamStmt{
		Stream: s,
	}

	return ss.Run(ctx)
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
