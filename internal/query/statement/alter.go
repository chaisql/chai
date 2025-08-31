package statement

import (
	"github.com/chaisql/chai/internal/database"
	errs "github.com/chaisql/chai/internal/errors"
	"github.com/chaisql/chai/internal/stream"
	"github.com/chaisql/chai/internal/stream/index"
	"github.com/chaisql/chai/internal/stream/table"
	"github.com/cockroachdb/errors"
)

var _ Statement = (*AlterTableRenameStmt)(nil)
var _ Statement = (*AlterTableAddColumnStmt)(nil)

// AlterTableRenameStmt is a DSL that allows creating a full ALTER TABLE query.
type AlterTableRenameStmt struct {
	TableName    string
	NewTableName string
}

// Run runs the ALTER TABLE statement in the given transaction.
// It implements the Statement interface.
func (stmt *AlterTableRenameStmt) Run(ctx *Context) (*Result, error) {
	if stmt.TableName == "" {
		return nil, errors.New("missing table name")
	}

	if stmt.NewTableName == "" {
		return nil, errors.New("missing new table name")
	}

	if stmt.TableName == stmt.NewTableName {
		return nil, errs.AlreadyExistsError{Name: stmt.NewTableName}
	}

	err := ctx.Conn.GetTx().CatalogWriter().RenameTable(ctx.Conn.GetTx(), stmt.TableName, stmt.NewTableName)
	return nil, err
}

type AlterTableAddColumnStmt struct {
	TableName        string
	ColumnConstraint *database.ColumnConstraint
	TableConstraints database.TableConstraints
}

// Run runs the ALTER TABLE ADD COLUMN statement in the given transaction.
// It implements the Statement interface.
// The statement rebuilds the table.
func (stmt *AlterTableAddColumnStmt) Run(ctx *Context) (*Result, error) {
	var err error

	// get the table before adding the column constraint
	// and assign the table to the table.Scan operator
	// so that it can decode the records properly
	scan := table.Scan(stmt.TableName)
	scan.Table, err = ctx.Conn.GetTx().Catalog.GetTable(ctx.Conn.GetTx(), stmt.TableName)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get table")
	}

	// get the current list of indexes
	indexNames := ctx.Conn.GetTx().Catalog.ListIndexes(stmt.TableName)

	// add the column constraint to the table
	err = ctx.Conn.GetTx().CatalogWriter().AddColumnConstraint(
		ctx.Conn.GetTx(),
		stmt.TableName,
		stmt.ColumnConstraint,
		stmt.TableConstraints)
	if err != nil {
		return nil, err
	}

	// create a unique index for every unique constraint
	pkAdded := false
	var newIdxs []*database.IndexInfo
	for _, tc := range stmt.TableConstraints {
		if tc.Unique {
			idx, err := ctx.Conn.GetTx().CatalogWriter().CreateIndex(ctx.Conn.GetTx(), &database.IndexInfo{
				Columns: tc.Columns,
				Unique:  true,
				Owner: database.Owner{
					TableName: stmt.TableName,
					Columns:   tc.Columns,
				},
			})
			if err != nil {
				return nil, err
			}

			newIdxs = append(newIdxs, idx)
		}

		if tc.PrimaryKey {
			pkAdded = true
		}
	}

	// create the stream:
	// on one side, scan the table with the old schema
	// on the other side, insert the records into the same table with the new schema
	s := stream.New(scan)

	// if a primary key was added, we need to delete the old records
	// and old indexes, and insert the new records and indexes
	if pkAdded {
		// delete the old records from the indexes
		for _, indexName := range indexNames {
			s = s.Pipe(index.Delete(indexName))
		}
		// delete the old records from the table
		s = s.Pipe(table.Delete(stmt.TableName))

		// validate the record against the new schema
		s = s.Pipe(table.Validate(stmt.TableName))

		// generate primary key
		s = s.Pipe(table.GenerateKey(stmt.TableName))

		// insert the record with the new primary key
		s = s.Pipe(table.Insert(stmt.TableName))

		// insert the record into the all the indexes
		indexNames = ctx.Conn.GetTx().Catalog.ListIndexes(stmt.TableName)
		for _, indexName := range indexNames {
			info, err := ctx.Conn.GetTx().Catalog.GetIndexInfo(indexName)
			if err != nil {
				return nil, err
			}
			if info.Unique {
				s = s.Pipe(index.Validate(indexName))
			}

			s = s.Pipe(index.Insert(indexName))
		}
	} else {
		// otherwise, we can just replace the old records with the new ones

		// validate the record against the new schema
		s = s.Pipe(table.Validate(stmt.TableName))

		// replace the old record with the new one
		s = s.Pipe(table.Replace(stmt.TableName))

		// update the new indexes only
		for _, idx := range newIdxs {
			if idx.Unique {
				s = s.Pipe(index.Validate(idx.IndexName))
			}

			s = s.Pipe(index.Insert(idx.IndexName))
		}
	}

	// ALTER TABLE ADD COLUMN does not return any result
	s = s.Pipe(stream.Discard())

	// do NOT optimize the stream
	return &Result{
		Result: &StreamStmtResult{
			Stream:  s,
			Context: ctx,
		},
	}, nil
}
