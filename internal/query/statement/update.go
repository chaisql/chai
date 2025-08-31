package statement

import (
	"github.com/chaisql/chai/internal/expr"
	"github.com/chaisql/chai/internal/stream"
	"github.com/chaisql/chai/internal/stream/index"
	"github.com/chaisql/chai/internal/stream/path"
	"github.com/chaisql/chai/internal/stream/rows"
	"github.com/chaisql/chai/internal/stream/table"
)

var _ Statement = (*UpdateStmt)(nil)

// UpdateConfig holds UPDATE configuration.
type UpdateStmt struct {
	PreparedStreamStmt

	TableName string

	// SetPairs is used along with the Set clause. It holds
	// each column with its corresponding value that
	// should be set in the row.
	SetPairs []UpdateSetPair

	WhereExpr expr.Expr
}

type UpdateSetPair struct {
	Column *expr.Column
	E      expr.Expr
}

func (stmt *UpdateStmt) Bind(ctx *Context) error {
	err := BindExpr(ctx, stmt.TableName, stmt.WhereExpr)
	if err != nil {
		return err
	}

	for i := range stmt.SetPairs {
		err = BindExpr(ctx, stmt.TableName, stmt.SetPairs[i].Column)
		if err != nil {
			return err
		}

		err = BindExpr(ctx, stmt.TableName, stmt.SetPairs[i].E)
		if err != nil {
			return err
		}
	}

	return nil
}

// Prepare implements the Preparer interface.
func (stmt *UpdateStmt) Prepare(c *Context) (Statement, error) {
	ti, err := c.Conn.GetTx().Catalog.GetTableInfo(stmt.TableName)
	if err != nil {
		return nil, err
	}
	pk := ti.PrimaryKey

	s := stream.New(table.Scan(stmt.TableName))

	if stmt.WhereExpr != nil {
		s = s.Pipe(rows.Filter(stmt.WhereExpr))
	}

	var pkModified bool
	if stmt.SetPairs != nil {
		for _, pair := range stmt.SetPairs {
			// if we modify the primary key,
			// we must remove the old row and create an new one
			if pk != nil && !pkModified {
				for _, c := range pk.Columns {
					if c == pair.Column.Name {
						pkModified = true
						break
					}
				}
			}
			s = s.Pipe(path.Set(pair.Column.Name, pair.E))
		}
	}

	// validate row
	s = s.Pipe(table.Validate(stmt.TableName))

	// TODO(asdine): This removes ALL indexed fields for each row
	// even if the update modified a single field. We should only
	// update the indexed fields that were modified.
	indexNames := c.Conn.GetTx().Catalog.ListIndexes(stmt.TableName)
	for _, indexName := range indexNames {
		s = s.Pipe(index.Delete(indexName))
	}

	if pkModified {
		s = s.Pipe(table.Delete(stmt.TableName))
		// generate primary key
		s = s.Pipe(table.GenerateKey(stmt.TableName))
		s = s.Pipe(table.Insert(stmt.TableName))
	} else {
		s = s.Pipe(table.Replace(stmt.TableName))
	}

	for _, indexName := range indexNames {
		info, err := c.Conn.GetTx().Catalog.GetIndexInfo(indexName)
		if err != nil {
			return nil, err
		}
		if info.Unique {
			s = s.Pipe(index.Validate(indexName))
		}

		s = s.Pipe(index.Insert(indexName))
	}

	s = s.Pipe(stream.Discard())

	stmt.PreparedStreamStmt.Stream = s
	return stmt, nil
}
