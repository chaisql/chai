package statement

import (
	"fmt"

	"github.com/cockroachdb/errors"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/internal/database"
	"github.com/genjidb/genji/internal/expr"
	"github.com/genjidb/genji/internal/sql/scanner"
	"github.com/genjidb/genji/internal/stream"
	"github.com/genjidb/genji/internal/stream/docs"
	"github.com/genjidb/genji/internal/stream/table"
)

type SelectCoreStmt struct {
	TableName       string
	Distinct        bool
	WhereExpr       expr.Expr
	GroupByExpr     expr.Expr
	ProjectionExprs []expr.Expr
}

// TODO: for draft placed here
func checkSchemaPathConstraints(tableInfo *database.TableInfo, src expr.Expr) error {
	var pathErr error
	expr.Walk(src, func(e expr.Expr) bool {
		epath, ok := e.(expr.Path)
		if !ok {
			return true
		}
		path := (document.Path)(epath)
		f := tableInfo.FieldConstraints
		for i := range path {
			fc, ok := f.ByField[path[i].FieldName]
			if !ok {
				if !f.AllowExtraFields {
					pathErr = fmt.Errorf("field name %q for path %q does not exists", path[i].FieldName, path)
					return false
				}
				return true
			}
			if fc.AnonymousType != nil {
				f = fc.AnonymousType.FieldConstraints
			}
			// if we encountered composite path, where some part before last
			// resolved in scalar type
			if fc.AnonymousType == nil && i != len(path)-1 {
				pathErr = fmt.Errorf("field name %q for path %q resolved in scalar type", path[i].FieldName, path)
				return false
			}
		}
		return true
	})
	return pathErr
}

func (stmt *SelectCoreStmt) Prepare(c *Context) (*StreamStmt, error) {
	isReadOnly := true

	var s *stream.Stream
	var tableInfo *database.TableInfo

	if stmt.TableName != "" {
		info, err := c.DB.Catalog.GetTableInfo(stmt.TableName)
		if err != nil {
			return nil, err
		}
		if !info.FieldConstraints.AllowExtraFields {
			tableInfo = info
		}
		s = s.Pipe(table.Scan(stmt.TableName))
	}

	if tableInfo != nil {
		for _, pe := range stmt.ProjectionExprs {
			ne, ok := pe.(*expr.NamedExpr)
			if !ok {
				// TODO: I didn't find other cases except NamedExpr, probably this should be exception?
				continue
			}
			if pathErr := checkSchemaPathConstraints(tableInfo, ne); pathErr != nil {
				return nil, pathErr
			}
			// TODO: see commennts in issue
			/*
				if ne.Name() != "" {
					// ...
				}
			*/
		}
	}

	if stmt.WhereExpr != nil {
		if tableInfo != nil {
			if pathErr := checkSchemaPathConstraints(tableInfo, stmt.WhereExpr); pathErr != nil {
				return nil, pathErr
			}
		}
		s = s.Pipe(docs.Filter(stmt.WhereExpr))
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
				// if so, replace the expression with a path expression
				stmt.ProjectionExprs[i] = &expr.NamedExpr{
					ExprName: ne.ExprName,
					Expr:     expr.Path(document.NewPath(e.String())),
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
		s = s.Pipe(docs.TempTreeSort(stmt.GroupByExpr))
		s = s.Pipe(docs.GroupAggregate(stmt.GroupByExpr, aggregators...))
	} else if stmt.TableName != "" {
		// if there is no GROUP BY clause, check if there are any aggregation function
		// and if so add an aggregation node
		var aggregators []expr.AggregatorBuilder

		for _, pe := range stmt.ProjectionExprs {
			ne, ok := pe.(*expr.NamedExpr)
			if !ok {
				continue
			}
			e := ne.Expr

			// check if the projected expression is an aggregation function
			if agg, ok := e.(expr.AggregatorBuilder); ok {
				aggregators = append(aggregators, agg)
			}
		}

		// add Aggregation node
		if len(aggregators) > 0 {
			s = s.Pipe(docs.GroupAggregate(nil, aggregators...))
		}
	}

	// If there is no FROM clause ensure there is no wildcard or path
	if stmt.TableName == "" {
		var err error

		for _, e := range stmt.ProjectionExprs {
			expr.Walk(e, func(e expr.Expr) bool {
				switch e.(type) {
				case expr.Path, expr.Wildcard:
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
	s = s.Pipe(docs.Project(stmt.ProjectionExprs...))

	// SELECT is read-only most of the time, unless it's using some expressions
	// that require write access and that are allowed to be run, such as NEXT VALUE FOR
	for _, e := range stmt.ProjectionExprs {
		expr.Walk(e, func(e expr.Expr) bool {
			switch e.(type) {
			case expr.NextValueFor:
				isReadOnly = false
				return false
			default:
				return true
			}
		})
	}

	if stmt.Distinct {
		s = stream.New(stream.Union(s))
	}

	return &StreamStmt{
		Stream:   s,
		ReadOnly: isReadOnly,
	}, nil
}

// SelectStmt holds SELECT configuration.
type SelectStmt struct {
	basePreparedStatement

	CompoundSelect    []*SelectCoreStmt
	CompoundOperators []scanner.Token
	OrderBy           expr.Path
	OrderByDirection  scanner.Token
	OffsetExpr        expr.Expr
	LimitExpr         expr.Expr
}

func NewSelectStatement() *SelectStmt {
	var p SelectStmt

	p.basePreparedStatement = basePreparedStatement{
		Preparer: &p,
		ReadOnly: true,
	}

	return &p
}

// Prepare implements the Preparer interface.
func (stmt *SelectStmt) Prepare(ctx *Context) (Statement, error) {
	var s *stream.Stream

	var prev scanner.Token

	var coreStmts []*stream.Stream
	var readOnly bool = true

	for i, coreSelect := range stmt.CompoundSelect {
		coreStmt, err := coreSelect.Prepare(ctx)
		if err != nil {
			return nil, err
		}

		if len(stmt.CompoundSelect) == 1 {
			s = coreStmt.Stream
			readOnly = coreStmt.ReadOnly
			break
		}

		coreStmts = append(coreStmts, coreStmt.Stream)

		if !coreStmt.ReadOnly {
			readOnly = false
		}

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
			s = s.Pipe(docs.TempTreeSortReverse(stmt.OrderBy))
		} else {
			s = s.Pipe(docs.TempTreeSort(stmt.OrderBy))
		}
	}

	if stmt.OffsetExpr != nil {
		s = s.Pipe(docs.Skip(stmt.OffsetExpr))
	}

	if stmt.LimitExpr != nil {
		s = s.Pipe(docs.Take(stmt.LimitExpr))
	}

	st := StreamStmt{
		Stream:   s,
		ReadOnly: readOnly,
	}

	return st.Prepare(ctx)
}
