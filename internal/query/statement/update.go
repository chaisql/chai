package statement

import (
	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/internal/expr"
	"github.com/genjidb/genji/internal/stream"
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
	s := stream.New(stream.SeqScan(stmt.TableName))

	if stmt.WhereExpr != nil {
		s = s.Pipe(stream.Filter(stmt.WhereExpr))
	}

	if stmt.SetPairs != nil {
		for _, pair := range stmt.SetPairs {
			s = s.Pipe(stream.Set(pair.Path, pair.E))
		}
	} else if stmt.UnsetFields != nil {
		for _, name := range stmt.UnsetFields {
			s = s.Pipe(stream.Unset(name))
		}
	}

	// validate document
	s = s.Pipe(stream.TableValidate(stmt.TableName))

	// TODO(asdine): This removes ALL indexed fields for each document
	// even if the update modified a single field. We should only
	// update the indexed fields that were modified.
	indexNames := c.Catalog.ListIndexes(stmt.TableName)
	for _, indexName := range indexNames {
		s = s.Pipe(stream.IndexDelete(indexName))
	}

	s = s.Pipe(stream.TableReplace(stmt.TableName))

	for _, indexName := range indexNames {
		s = s.Pipe(stream.IndexInsert(indexName))
	}

	st := StreamStmt{
		Stream:   s,
		ReadOnly: false,
	}

	return st.Prepare(c)
}
