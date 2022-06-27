package statement

import (
	"github.com/cockroachdb/errors"
	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/internal/expr"
	"github.com/genjidb/genji/internal/stream"
	"github.com/genjidb/genji/internal/stream/docs"
	"github.com/genjidb/genji/internal/stream/index"
	"github.com/genjidb/genji/internal/stream/path"
	"github.com/genjidb/genji/internal/stream/table"
)

// UpdateConfig holds UPDATE configuration.
type UpdateStmt struct {
	basePreparedStatement

	TableName string

	// SetPairs is used along with the Set clause. It holds
	// each path with its corresponding value that
	// should be set in the document.
	SetPairs []UpdateSetPair

	// UnsetFields is used along with the Unset clause. It holds
	// each path that should be unset from the document.
	UnsetFields []string

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
	Path document.Path
	E    expr.Expr
}

// Prepare implements the Preparer interface.
func (stmt *UpdateStmt) Prepare(c *Context) (Statement, error) {
	ti, err := c.Catalog.GetTableInfo(stmt.TableName)
	if err != nil {
		return nil, err
	}
	pk := ti.GetPrimaryKey()

	s := stream.New(table.Scan(stmt.TableName))

	if stmt.WhereExpr != nil {
		s = s.Pipe(docs.Filter(stmt.WhereExpr))
	}

	var pkModified bool
	if stmt.SetPairs != nil {
		for _, pair := range stmt.SetPairs {
			// if we modify the primary key,
			// we must remove the old document and create an new one
			if pk != nil && !pkModified {
				for _, p := range pk.Paths {
					if p.IsEqual(pair.Path) {
						pkModified = true
						break
					}
				}
			}
			s = s.Pipe(path.Set(pair.Path, pair.E))
		}
	} else if stmt.UnsetFields != nil {
		for _, name := range stmt.UnsetFields {
			// ensure we do not unset any path the is used in the primary key
			if pk != nil {
				path := document.NewPath(name)
				for _, p := range pk.Paths {
					if p.IsEqual(path) {
						return nil, errors.New("cannot unset primary key path")
					}
				}
			}
			s = s.Pipe(path.Unset(name))
		}
	}

	// validate document
	s = s.Pipe(table.Validate(stmt.TableName))

	// TODO(asdine): This removes ALL indexed fields for each document
	// even if the update modified a single field. We should only
	// update the indexed fields that were modified.
	indexNames := c.Catalog.ListIndexes(stmt.TableName)
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
		s = s.Pipe(index.IndexInsert(indexName))
	}

	s = s.Pipe(stream.Discard())

	st := StreamStmt{
		Stream:   s,
		ReadOnly: false,
	}

	return st.Prepare(c)
}
