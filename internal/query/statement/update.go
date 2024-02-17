package statement

import (
	"github.com/chaisql/chai/internal/expr"
	"github.com/chaisql/chai/internal/stream"
	"github.com/chaisql/chai/internal/stream/index"
	"github.com/chaisql/chai/internal/stream/path"
	"github.com/chaisql/chai/internal/stream/rows"
	"github.com/chaisql/chai/internal/stream/table"
)

// UpdateConfig holds UPDATE configuration.
type UpdateStmt struct {
	basePreparedStatement

	TableName string

	// SetPairs is used along with the Set clause. It holds
	// each column with its corresponding value that
	// should be set in the row.
	SetPairs []UpdateSetPair

	WhereExpr expr.Expr
}

func NewUpdateStatement() *UpdateStmt {
	var p UpdateStmt

	p.basePreparedStatement = basePreparedStatement{
		Preparer: &p,
		ReadOnly: false,
	}

	return &p
}

type UpdateSetPair struct {
	Column expr.Column
	E      expr.Expr
}

// Prepare implements the Preparer interface.
func (stmt *UpdateStmt) Prepare(c *Context) (Statement, error) {
	ti, err := c.Tx.Catalog.GetTableInfo(stmt.TableName)
	if err != nil {
		return nil, err
	}
	pk := ti.PrimaryKey

	s := stream.New(table.Scan(stmt.TableName))

	if stmt.WhereExpr != nil {
		err := ensureExprColumnsExist(c, stmt.TableName, stmt.WhereExpr)
		if err != nil {
			return nil, err
		}

		s = s.Pipe(rows.Filter(stmt.WhereExpr))
	}

	var pkModified bool
	if stmt.SetPairs != nil {
		for _, pair := range stmt.SetPairs {
			err := ensureExprColumnsExist(c, stmt.TableName, pair.Column)
			if err != nil {
				return nil, err
			}

			// if we modify the primary key,
			// we must remove the old row and create an new one
			if pk != nil && !pkModified {
				for _, c := range pk.Columns {
					if c == string(pair.Column) {
						pkModified = true
						break
					}
				}
			}
			s = s.Pipe(path.Set(string(pair.Column), pair.E))
		}
	}

	// validate row
	s = s.Pipe(table.Validate(stmt.TableName))

	// TODO(asdine): This removes ALL indexed fields for each row
	// even if the update modified a single field. We should only
	// update the indexed fields that were modified.
	indexNames := c.Tx.Catalog.ListIndexes(stmt.TableName)
	for _, indexName := range indexNames {
		s = s.Pipe(index.Delete(indexName))
	}

	if pkModified {
		s = s.Pipe(table.Delete(stmt.TableName))
		s = s.Pipe(table.Insert(stmt.TableName))
	} else {
		s = s.Pipe(table.Replace(stmt.TableName))
	}

	for _, indexName := range indexNames {
		info, err := c.Tx.Catalog.GetIndexInfo(indexName)
		if err != nil {
			return nil, err
		}
		if info.Unique {
			s = s.Pipe(index.Validate(indexName))
		}

		s = s.Pipe(index.Insert(indexName))
	}

	s = s.Pipe(stream.Discard())

	st := StreamStmt{
		Stream:   s,
		ReadOnly: false,
	}

	return st.Prepare(c)
}
