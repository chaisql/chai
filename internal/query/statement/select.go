package statement

import (
	"fmt"

	"github.com/chaisql/chai/internal/expr"
	"github.com/chaisql/chai/internal/expr/functions"
	"github.com/chaisql/chai/internal/sql/scanner"
	"github.com/chaisql/chai/internal/stream"
	"github.com/chaisql/chai/internal/stream/rows"
	"github.com/chaisql/chai/internal/stream/table"
	"github.com/cockroachdb/errors"
)

var _ Statement = (*SelectStmt)(nil)

type SelectCoreStmt struct {
	TableName       string
	Distinct        bool
	WhereExpr       expr.Expr
	GroupByExpr     expr.Expr
	ProjectionExprs []expr.Expr
}

func (stmt *SelectCoreStmt) Bind(ctx *Context) error {
	err := BindExpr(ctx, stmt.TableName, stmt.WhereExpr)
	if err != nil {
		return err
	}

	err = BindExpr(ctx, stmt.TableName, stmt.GroupByExpr)
	if err != nil {
		return err
	}

	for i := range stmt.ProjectionExprs {
		err = BindExpr(ctx, stmt.TableName, stmt.ProjectionExprs[i])
		if err != nil {
			return err
		}
	}

	return nil
}

func (stmt *SelectCoreStmt) IsReadOnly() bool {
	var isReadOnly = true

	// SELECT is read-only most of the time, unless it's using some expressions
	// that require write access and that are allowed to be run, such as nextval
	for _, e := range stmt.ProjectionExprs {
		expr.Walk(e, func(e expr.Expr) bool {
			switch e.(type) {
			case *expr.NamedExpr:
				return true
			case *functions.NextVal:
				isReadOnly = false
				return false
			default:
				return true
			}
		})
	}

	return isReadOnly
}

func (stmt *SelectCoreStmt) Prepare(ctx *Context) (*stream.Stream, error) {
	var s *stream.Stream

	if stmt.TableName != "" {
		_, err := ctx.Conn.GetTx().Catalog.GetTableInfo(stmt.TableName)
		if err != nil {
			return nil, err
		}

		s = s.Pipe(table.Scan(stmt.TableName))
	}

	if stmt.WhereExpr != nil {
		s = s.Pipe(rows.Filter(stmt.WhereExpr))
	}

	// when using GROUP BY, only aggregation functions or GroupByExpr can be selected
	if stmt.GroupByExpr != nil {
		var invalidProjectedField expr.Expr
		var aggregators []expr.AggregatorBuilder

		for i, pe := range stmt.ProjectionExprs {
			ne, ok := pe.(*expr.NamedExpr)
			if !ok {
				invalidProjectedField = pe
				break
			}
			e := ne.Expr

			// check if the projected expression is an aggregation function
			if agg, ok := e.(expr.AggregatorBuilder); ok {
				aggregators = append(aggregators, agg)
				continue
			}

			// check if this is the same expression as the one used in the GROUP BY clause
			if expr.Equal(e, stmt.GroupByExpr) {
				// if so, replace the expression with a column expression
				stmt.ProjectionExprs[i] = &expr.NamedExpr{
					ExprName: ne.ExprName,
					Expr: &expr.Column{
						Name:  e.String(),
						Table: stmt.TableName,
					},
				}
				continue
			}

			// otherwise it's an error
			invalidProjectedField = ne
			break
		}

		if invalidProjectedField != nil {
			return nil, fmt.Errorf("field %q must appear in the GROUP BY clause or be used in an aggregate function", invalidProjectedField)
		}
		// add Aggregation node
		s = s.Pipe(rows.TempTreeSort(stmt.GroupByExpr))
		s = s.Pipe(rows.GroupAggregate(stmt.GroupByExpr, aggregators...))
	} else if stmt.TableName != "" {
		// if there is no GROUP BY clause, check if there are any aggregation function
		// and if so add an aggregation node
		var aggregators []expr.AggregatorBuilder

		for _, pe := range stmt.ProjectionExprs {
			expr.Walk(pe, func(e expr.Expr) bool {
				// check if the projected expression contains an aggregation function
				if agg, ok := e.(expr.AggregatorBuilder); ok {
					aggregators = append(aggregators, agg)
					return true
				}

				return true
			})
		}

		// add Aggregation node
		if len(aggregators) > 0 {
			s = s.Pipe(rows.GroupAggregate(nil, aggregators...))
		}
	}

	// If there is no FROM clause ensure there is no wildcard or path
	if stmt.TableName == "" {
		var err error

		for _, e := range stmt.ProjectionExprs {
			expr.Walk(e, func(e expr.Expr) bool {
				switch e.(type) {
				case *expr.Column, expr.Wildcard:
					err = errors.New("no tables specified")
					return false
				default:
					return true
				}
			})
			if err != nil {
				return nil, err
			}
		}
	}
	s = s.Pipe(rows.Project(stmt.ProjectionExprs...))

	if stmt.Distinct {
		s = stream.New(stream.Union(s))
	}

	return s, nil
}

// SelectStmt holds SELECT configuration.
type SelectStmt struct {
	PreparedStreamStmt

	CompoundSelect    []*SelectCoreStmt
	CompoundOperators []scanner.Token
	OrderBy           *expr.Column
	OrderByDirection  scanner.Token
	OffsetExpr        expr.Expr
	LimitExpr         expr.Expr
}

func (stmt *SelectStmt) IsReadOnly() bool {
	for i := range stmt.CompoundSelect {
		if !stmt.CompoundSelect[i].IsReadOnly() {
			return false
		}
	}
	return true
}

func (stmt *SelectStmt) Bind(ctx *Context) error {
	for i := range stmt.CompoundSelect {
		err := stmt.CompoundSelect[i].Bind(ctx)
		if err != nil {
			return err
		}
	}

	err := BindExpr(ctx, stmt.CompoundSelect[0].TableName, stmt.OrderBy)
	if err != nil {
		return err
	}

	err = BindExpr(ctx, stmt.CompoundSelect[0].TableName, stmt.OffsetExpr)
	if err != nil {
		return err
	}

	err = BindExpr(ctx, stmt.CompoundSelect[0].TableName, stmt.LimitExpr)
	if err != nil {
		return err
	}

	return nil
}

// Prepare implements the Preparer interface.
func (stmt *SelectStmt) Prepare(ctx *Context) (Statement, error) {
	var s *stream.Stream

	var prev scanner.Token

	var coreStmts []*stream.Stream

	for i, coreSelect := range stmt.CompoundSelect {
		coreStmt, err := coreSelect.Prepare(ctx)
		if err != nil {
			return nil, err
		}

		if len(stmt.CompoundSelect) == 1 {
			s = coreStmt
			break
		}

		coreStmts = append(coreStmts, coreStmt)

		var tok scanner.Token
		if i < len(stmt.CompoundOperators) {
			tok = stmt.CompoundOperators[i]
		}

		if prev != 0 && prev != tok {
			switch prev {
			case scanner.UNION:
				s = stream.New(stream.Union(coreStmts...))
			case scanner.ALL:
				s = stream.New(stream.Concat(coreStmts...))
			}

			coreStmts = []*stream.Stream{s}
		}

		prev = tok
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

	stmt.PreparedStreamStmt.Stream = s
	return stmt, nil
}
