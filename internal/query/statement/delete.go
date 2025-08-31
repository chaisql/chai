package statement

import (
	"github.com/chaisql/chai/internal/expr"
	"github.com/chaisql/chai/internal/sql/scanner"
	"github.com/chaisql/chai/internal/stream"
	"github.com/chaisql/chai/internal/stream/index"
	"github.com/chaisql/chai/internal/stream/rows"
	"github.com/chaisql/chai/internal/stream/table"
)

var _ Statement = (*DeleteStmt)(nil)

// DeleteConfig holds DELETE configuration.
type DeleteStmt struct {
	PreparedStreamStmt

	TableName        string
	WhereExpr        expr.Expr
	OffsetExpr       expr.Expr
	OrderBy          *expr.Column
	LimitExpr        expr.Expr
	OrderByDirection scanner.Token
}

func (stmt *DeleteStmt) Bind(ctx *Context) error {
	err := BindExpr(ctx, stmt.TableName, stmt.WhereExpr)
	if err != nil {
		return err
	}

	err = BindExpr(ctx, stmt.TableName, stmt.OffsetExpr)
	if err != nil {
		return err
	}

	err = BindExpr(ctx, stmt.TableName, stmt.OrderBy)
	if err != nil {
		return err
	}

	err = BindExpr(ctx, stmt.TableName, stmt.LimitExpr)
	if err != nil {
		return err
	}

	return nil
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

	indexNames := c.Conn.GetTx().Catalog.ListIndexes(stmt.TableName)
	for _, indexName := range indexNames {
		s = s.Pipe(index.Delete(indexName))
	}

	s = s.Pipe(table.Delete(stmt.TableName))

	s = s.Pipe(stream.Discard())

	stmt.PreparedStreamStmt.Stream = s
	return stmt, nil
}
