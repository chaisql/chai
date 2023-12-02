package statement

import (
	"github.com/chaisql/chai/internal/expr"
	"github.com/chaisql/chai/internal/sql/scanner"
	"github.com/chaisql/chai/internal/stream"
	"github.com/chaisql/chai/internal/stream/index"
	"github.com/chaisql/chai/internal/stream/rows"
	"github.com/chaisql/chai/internal/stream/table"
)

// DeleteConfig holds DELETE configuration.
type DeleteStmt struct {
	basePreparedStatement

	TableName        string
	WhereExpr        expr.Expr
	OffsetExpr       expr.Expr
	OrderBy          expr.Path
	LimitExpr        expr.Expr
	OrderByDirection scanner.Token
}

func NewDeleteStatement() *DeleteStmt {
	var p DeleteStmt

	p.basePreparedStatement = basePreparedStatement{
		Preparer: &p,
		ReadOnly: false,
	}

	return &p
}

func (stmt *DeleteStmt) Prepare(c *Context) (Statement, error) {
	s := stream.New(table.Scan(stmt.TableName))

	if stmt.WhereExpr != nil {
		s = s.Pipe(rows.Filter(stmt.WhereExpr))
	}

	if stmt.OrderBy != nil {
		if stmt.OrderByDirection == scanner.DESC {
			s = s.Pipe(rows.TempTreeSortReverse(stmt.OrderBy))
		} else {
			s = s.Pipe(rows.TempTreeSort(stmt.OrderBy))
		}
	}

	if stmt.OffsetExpr != nil {
		s = s.Pipe(rows.Skip(stmt.OffsetExpr))
	}

	if stmt.LimitExpr != nil {
		s = s.Pipe(rows.Take(stmt.LimitExpr))
	}

	indexNames := c.Tx.Catalog.ListIndexes(stmt.TableName)
	for _, indexName := range indexNames {
		s = s.Pipe(index.Delete(indexName))
	}

	s = s.Pipe(table.Delete(stmt.TableName))

	s = s.Pipe(stream.Discard())

	st := StreamStmt{
		Stream:   s,
		ReadOnly: false,
	}

	return st.Prepare(c)
}
