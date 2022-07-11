package statement

import (
	"fmt"
	"strings"

	"github.com/cockroachdb/errors"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/internal/database"
	"github.com/genjidb/genji/internal/expr"
	"github.com/genjidb/genji/internal/sql/scanner"
	"github.com/genjidb/genji/internal/stream"
	"github.com/genjidb/genji/internal/stream/docs"
	"github.com/genjidb/genji/internal/stream/table"
	"github.com/genjidb/genji/types"
)

type SelectCoreStmt struct {
	TableName       string
	Distinct        bool
	WhereExpr       expr.Expr
	GroupByExpr     expr.Expr
	ProjectionExprs []expr.Expr
}

func (stmt *SelectCoreStmt) analyzeExpr(ti *database.TableInfo, e expr.Expr) error {
	if ti.FieldConstraints.AllowExtraFields {
		return nil
	}

	var (
		prev scanner.Token
		path string
	)

	scan := scanner.NewScanner(strings.NewReader(e.String()))
	for {
		tok, _, lit := scan.Scan()
		switch tok {
		case scanner.IDENT:
			path += lit
		case scanner.DOT:
			path += "."
		case scanner.WS:
			if prev == scanner.IDENT {
				err := analyzePath(strings.Split(path, "."), ti.FieldConstraints)
				if err != nil {
					return err
				}
			}
			path = ""
		case scanner.EOF:
			if prev == scanner.IDENT {
				return analyzePath(strings.Split(path, "."), ti.FieldConstraints)
			}
			return nil
		}

		prev = tok
	}

}

func analyzePath(path []string, fc database.FieldConstraints) error {
	if fc.AllowExtraFields {
		return nil
	}

	f, ok := fc.ByField[path[0]]
	if !ok {
		return types.ErrFieldNotFound
	}

	if len(path) == 1 {
		return nil
	}

	return analyzePath(path[1:], f.AnonymousType.FieldConstraints)
}

func (stmt *SelectCoreStmt) Prepare(ctx *Context) (*StreamStmt, error) {
	isReadOnly := true

	var (
		s   *stream.Stream
		ti  *database.TableInfo
		err error
	)

	if stmt.TableName != "" {
		ti, err = ctx.Catalog.GetTableInfo(stmt.TableName)
		if err != nil {
			return nil, err
		}

		for _, pe := range stmt.ProjectionExprs {
			var err error
			expr.Walk(pe, func(e expr.Expr) bool {
				switch e.(type) {
				case expr.Path:
					err = analyzePath(strings.Split(e.String(), "."), ti.FieldConstraints)
					if err != nil {
						return false
					}

					return true
				default:
					return true
				}
			})
			if err != nil {
				return nil, err
			}
		}

		s = s.Pipe(table.Scan(stmt.TableName))
	}

	if stmt.WhereExpr != nil {
		err := stmt.analyzeExpr(ti, stmt.WhereExpr)
		if err != nil {
			return nil, err
		}

		s = s.Pipe(docs.Filter(stmt.WhereExpr))
	}

	// when using GROUP BY, only aggregation functions or GroupByExpr can be selected
	if stmt.GroupByExpr != nil {
		err := stmt.analyzeExpr(ti, stmt.GroupByExpr)
		if err != nil {
			return nil, err
		}

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
	var (
		coreStmts []*stream.Stream
		s         *stream.Stream
		prev      scanner.Token
		errStmt   []error
		readOnly  bool
	)

	readOnly = true
	for i, coreSelect := range stmt.CompoundSelect {
		coreStmt, err := coreSelect.Prepare(ctx)
		if err != nil {
			return nil, err
		}

		if stmt.OrderBy != nil {
			ti, err := ctx.Catalog.GetTableInfo(coreSelect.TableName)
			if err != nil {
				return nil, err
			}

			err = coreSelect.analyzeExpr(ti, stmt.OrderBy)
			if err != nil {
				errStmt = append(errStmt, err)
			}
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
		if len(errStmt) == len(stmt.CompoundSelect) {
			return nil, errStmt[0]
		}

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
