package statement

import (
	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/internal/environment"
	"github.com/genjidb/genji/internal/expr"
	"github.com/genjidb/genji/internal/sql/scanner"
	"github.com/genjidb/genji/internal/stream"
	"github.com/genjidb/genji/internal/stringutil"
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
	s := stream.New(stream.SeqScan(stmt.TableName))

	if stmt.WhereExpr != nil {
		s = s.Pipe(stream.Filter(stmt.WhereExpr))
	}

	if stmt.OrderBy != nil {
		if stmt.OrderByDirection == scanner.DESC {
			s = s.Pipe(stream.TempTreeSortReverse(stmt.OrderBy))
		} else {
			s = s.Pipe(stream.TempTreeSort(stmt.OrderBy))
		}
	}

	if stmt.OffsetExpr != nil {
		v, err := stmt.OffsetExpr.Eval(&environment.Environment{})
		if err != nil {
			return nil, err
		}

		if !v.Type().IsNumber() {
			return nil, stringutil.Errorf("offset expression must evaluate to a number, got %q", v.Type())
		}

		v, err = document.CastAsInteger(v)
		if err != nil {
			return nil, err
		}

		s = s.Pipe(stream.Skip(v.V().(int64)))
	}

	if stmt.LimitExpr != nil {
		v, err := stmt.LimitExpr.Eval(&environment.Environment{})
		if err != nil {
			return nil, err
		}

		if !v.Type().IsNumber() {
			return nil, stringutil.Errorf("limit expression must evaluate to a number, got %q", v.Type())
		}

		v, err = document.CastAsInteger(v)
		if err != nil {
			return nil, err
		}

		s = s.Pipe(stream.Take(v.V().(int64)))
	}

	indexNames := c.Catalog.ListIndexes(stmt.TableName)
	for _, indexName := range indexNames {
		s = s.Pipe(stream.IndexDelete(indexName))
	}

	s = s.Pipe(stream.TableDelete(stmt.TableName))

	st := StreamStmt{
		Stream:   s,
		ReadOnly: false,
	}

	return st.Prepare(c)
}
