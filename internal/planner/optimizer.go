package planner

import (
	"github.com/chaisql/chai/internal/database"
	"github.com/chaisql/chai/internal/environment"
	"github.com/chaisql/chai/internal/expr"
	"github.com/chaisql/chai/internal/sql/scanner"
	"github.com/chaisql/chai/internal/stream"
	"github.com/chaisql/chai/internal/stream/path"
	"github.com/chaisql/chai/internal/stream/rows"
	"github.com/chaisql/chai/internal/stream/table"
	"github.com/chaisql/chai/internal/types"
	"github.com/cockroachdb/errors"
)

var optimizerRules = []func(sctx *StreamContext) error{
	SplitANDConditionRule,
	PrecalculateExprRule,
	RemoveUnnecessaryProjection,
	RemoveUnnecessaryFilterNodesRule,
	RemoveUnnecessaryTempSortNodesRule,
	SelectIndex,
}

// Optimize takes a tree, applies a list of optimization rules
// and returns an optimized tree.
// Depending on the rule, the tree may be modified in place or
// replaced by a new one.
func Optimize(s *stream.Stream, catalog *database.Catalog, params []environment.Param) (*stream.Stream, error) {
	if firstNode, ok := s.First().(*stream.ConcatOperator); ok {
		// If the first operation is a concat, optimize all streams individually.
		for i, st := range firstNode.Streams {
			ss, err := Optimize(st, catalog, params)
			if err != nil {
				return nil, err
			}
			firstNode.Streams[i] = ss
		}

		return s, nil
	}

	if firstNode, ok := s.First().(*stream.UnionOperator); ok {
		// If the first operation is a union, optimize all streams individually.
		for i, st := range firstNode.Streams {
			ss, err := Optimize(st, catalog, params)
			if err != nil {
				return nil, err
			}
			firstNode.Streams[i] = ss
		}

		return s, nil
	}

	return optimize(s, catalog, params)
}

type StreamContext struct {
	Catalog       *database.Catalog
	TableInfo     *database.TableInfo
	Params        []environment.Param
	Stream        *stream.Stream
	Filters       []*rows.FilterOperator
	Projections   []*rows.ProjectOperator
	TempTreeSorts []*rows.TempTreeSortOperator
}

func NewStreamContext(s *stream.Stream, catalog *database.Catalog) *StreamContext {
	sctx := StreamContext{
		Stream:  s,
		Catalog: catalog,
	}

	n := s.First()

	prevIsFilter := false

	for n != nil {
		switch t := n.(type) {
		case *table.ScanOperator:
			if catalog != nil {
				ti, err := sctx.Catalog.GetTableInfo(t.TableName)
				if err != nil {
					panic(err)
				}
				sctx.TableInfo = ti
			}
		case *rows.FilterOperator:
			if prevIsFilter || len(sctx.Filters) == 0 {
				sctx.Filters = append(sctx.Filters, t)
				prevIsFilter = true
			}
		case *rows.ProjectOperator:
			sctx.Projections = append(sctx.Projections, t)
			prevIsFilter = false
		case *rows.TempTreeSortOperator:
			sctx.TempTreeSorts = append(sctx.TempTreeSorts, t)
			prevIsFilter = false
		}

		n = n.GetNext()
	}

	return &sctx
}

func (sctx *StreamContext) removeFilterNodeByIndex(index int) {
	f := sctx.Filters[index]
	sctx.Stream.Remove(f)
	sctx.Filters = append(sctx.Filters[:index], sctx.Filters[index+1:]...)
}

func (sctx *StreamContext) removeFilterNode(f *rows.FilterOperator) {
	for i, flt := range sctx.Filters {
		if flt == f {
			sctx.removeFilterNodeByIndex(i)
			return
		}
	}
}

func (sctx *StreamContext) removeTempTreeNodeByIndex(index int) {
	f := sctx.TempTreeSorts[index]
	sctx.Stream.Remove(f)
	sctx.TempTreeSorts = append(sctx.TempTreeSorts[:index], sctx.TempTreeSorts[index+1:]...)
}

func (sctx *StreamContext) removeTempTreeNodeNode(f *rows.TempTreeSortOperator) {
	for i, flt := range sctx.TempTreeSorts {
		if flt == f {
			sctx.removeTempTreeNodeByIndex(i)
			return
		}
	}
}

func (sctx *StreamContext) removeProjectionNode(index int) {
	p := sctx.Projections[index]

	sctx.Stream.Remove(p)
	sctx.Projections = append(sctx.Projections[:index], sctx.Projections[index+1:]...)
}

func optimize(s *stream.Stream, catalog *database.Catalog, params []environment.Param) (*stream.Stream, error) {
	sctx := NewStreamContext(s, catalog)
	sctx.Params = params

	for _, rule := range optimizerRules {
		err := rule(sctx)
		if err != nil {
			return nil, err
		}
		if sctx.Stream == nil || sctx.Stream.Op == nil {
			break
		}
	}

	return sctx.Stream, nil
}

// SplitANDConditionRule splits any filter node whose condition
// is one or more AND operators into one or more filter nodes.
// The condition won't be split if the expression tree contains an OR
// operation.
// Example:
//
//	this:
//	  rows.Filter(a > 2 AND b != 3 AND c < 2)
//	becomes this:
//	  rows.Filter(a > 2)
//	  rows.Filter(b != 3)
//	  rows.Filter(c < 2)
func SplitANDConditionRule(sctx *StreamContext) error {
	for i, f := range sctx.Filters {
		cond := f.Expr
		if cond == nil {
			continue
		}
		// The AND operator has one of the lowest precedence,
		// only OR has a lower precedence,
		// which means that if AND is used without OR, it will be at
		// the top of the expression tree.
		if op, ok := cond.(expr.Operator); ok && op.Token() == scanner.AND {
			exprs := splitANDExpr(cond)

			cur := f.GetPrev()

			// create new filter nodes and add them to the stream
			for _, e := range exprs {
				newF := rows.Filter(e)
				cur = stream.InsertAfter(cur, newF)
				sctx.Filters = append(sctx.Filters, newF)
			}

			// remove the current expression from the stream
			sctx.removeFilterNodeByIndex(i)

			if sctx.Stream.Op == nil {
				sctx.Stream.Op = cur
			}
		}
	}

	return nil
}

// splitANDExpr takes an expression and splits it by AND operator.
func splitANDExpr(cond expr.Expr) (exprs []expr.Expr) {
	op, ok := cond.(expr.Operator)
	if ok && op.Token() == scanner.AND {
		exprs = append(exprs, splitANDExpr(op.LeftHand())...)
		exprs = append(exprs, splitANDExpr(op.RightHand())...)
		return
	}

	exprs = append(exprs, cond)
	return
}

// PrecalculateExprRule evaluates any constant sub-expression that can be evaluated
// before running the query and replaces it by the result of the evaluation.
// The result of constant sub-expressions, like "3 + 4", is always the same and thus
// can be precalculated.
// Examples:
//
//	3 + 4 --> 7
//	3 + 1 > 10 - a --> 4 > 10 - a
func PrecalculateExprRule(sctx *StreamContext) error {
	n := sctx.Stream.Op
	var err error

	for n != nil {
		switch t := n.(type) {
		case *rows.FilterOperator:
			t.Expr, err = precalculateExpr(sctx, t.Expr)
		case *rows.ProjectOperator:
			for i := range t.Exprs {
				t.Exprs[i], err = precalculateExpr(sctx, t.Exprs[i])
				if err != nil {
					return err
				}
			}
		case *rows.TempTreeSortOperator:
			t.Expr, err = precalculateExpr(sctx, t.Expr)
		case *path.SetOperator:
			t.Expr, err = precalculateExpr(sctx, t.Expr)
		case *rows.EmitOperator:
			for i := range t.Rows {
				e, err := precalculateExpr(sctx, expr.LiteralExprList(t.Rows[i].Exprs))
				if err != nil {
					return err
				}
				t.Rows[i].Exprs = e.(expr.LiteralExprList)
			}
		}

		if err != nil {
			return err
		}

		n = n.GetPrev()
	}

	return err
}

// precalculateExpr is a recursive function that tries to precalculate
// expression nodes when possible.
// it returns a new expression with simplified nodes.
// if no simplification is possible it returns the same expression.
func precalculateExpr(sctx *StreamContext, e expr.Expr) (expr.Expr, error) {
	switch t := e.(type) {
	case expr.LiteralExprList:
		// we assume that the list of expressions contains only literals
		// until proven wrong.
		for i, te := range t {
			newExpr, err := precalculateExpr(sctx, te)
			if err != nil {
				return nil, err
			}
			t[i] = newExpr
		}
	case expr.PositionalParam, expr.NamedParam:
		v, err := t.Eval(environment.New(nil, nil, sctx.Params, nil))
		if err != nil {
			return nil, err
		}
		return expr.LiteralValue{Value: v}, nil
	case expr.Operator:
		// since expr.Operator is an interface,
		// this optimization must only be applied to
		// a few selected operators that we know about.
		tok := t.Token()
		if tok != scanner.AND &&
			tok != scanner.OR &&
			!expr.IsArithmeticOperator(t) &&
			!expr.IsComparisonOperator(t) {
			return e, nil
		}

		lh, err := precalculateExpr(sctx, t.LeftHand())
		if err != nil {
			return nil, err
		}
		rh, err := precalculateExpr(sctx, t.RightHand())
		if err != nil {
			return nil, err
		}
		t.SetLeftHandExpr(lh)
		t.SetRightHandExpr(rh)

		if b, ok := t.(*expr.BetweenOperator); ok {
			b.X, err = precalculateExpr(sctx, b.X)
			if err != nil {
				return nil, err
			}

			if _, isLit := b.X.(expr.LiteralValue); !isLit {
				break
			}
		}

		lv, leftIsLit := lh.(expr.LiteralValue)
		rv, rightIsLit := rh.(expr.LiteralValue)
		// if both operands are literals, we can precalculate them now
		if leftIsLit && rightIsLit {
			v, err := t.Eval(&environment.Environment{})
			if err != nil {
				return nil, err
			}
			// we replace this expression with the result of its evaluation
			return expr.LiteralValue{Value: v}, nil
		}

		// if one operand is a column and the other is a literal
		// we can check if the types are compatible
		lc, leftIsCol := lh.(*expr.Column)
		rc, rightIsCol := rh.(*expr.Column)

		if leftIsCol && rightIsLit {
			tp := sctx.TableInfo.ColumnConstraints.GetColumnConstraint(lc.Name).Type
			if !tp.Def().IsComparableWith(rv.Value.Type()) {
				return nil, errors.Errorf("invalid input syntax for type %s: %s", tp, rh)
			}

			if tp.Def().IsIndexComparableWith(rv.Value.Type()) {
				v, err := rv.Value.CastAs(tp)
				if err != nil {
					return nil, errors.Errorf("invalid input syntax for type %s: %s", tp, rh)
				}
				t.SetRightHandExpr(expr.LiteralValue{Value: v})
			}
		}

		if leftIsLit && rightIsCol {
			tp := sctx.TableInfo.ColumnConstraints.GetColumnConstraint(rc.Name).Type
			if !tp.Def().IsComparableWith(lv.Value.Type()) {
				return nil, errors.Errorf("invalid input syntax for type %s: %s", tp, lh)
			}

			if tp.Def().IsIndexComparableWith(lv.Value.Type()) {
				v, err := lv.Value.CastAs(tp)
				if err != nil {
					return nil, errors.Errorf("invalid input syntax for type %s: %s", tp, lh)
				}
				t.SetLeftHandExpr(expr.LiteralValue{Value: v})
			}
		}

		return t, nil
	}

	return e, nil
}

func CheckExprTypeRule(sctx *StreamContext) error {
	n := sctx.Stream.Op
	var err error

	for n != nil {
		switch t := n.(type) {
		case *rows.FilterOperator:
			err = checkExprType(sctx, t.Expr)
		case *rows.ProjectOperator:
			for i := range t.Exprs {
				err = checkExprType(sctx, t.Exprs[i])
				if err != nil {
					return err
				}
			}
		case *rows.TempTreeSortOperator:
			err = checkExprType(sctx, t.Expr)
		case *path.SetOperator:
			err = checkExprType(sctx, t.Expr)
		case *rows.EmitOperator:
			for i := range t.Rows {
				err := checkExprType(sctx, expr.LiteralExprList(t.Rows[i].Exprs))
				if err != nil {
					return err
				}
			}
		}

		if err != nil {
			return err
		}

		n = n.GetPrev()
	}

	return err
}

func checkExprType(sctx *StreamContext, e expr.Expr) (err error) {
	op, ok := e.(expr.Operator)
	if !ok {
		return nil
	}

	lh := op.LeftHand()
	rh := op.RightHand()

	lc, leftIsCol := lh.(*expr.Column)
	rc, rightIsCol := rh.(*expr.Column)

	lv, leftIsLit := lh.(expr.LiteralValue)
	rv, rightIsLit := rh.(expr.LiteralValue)

	if leftIsCol && rightIsCol {
		return nil
	}

	if leftIsCol && rightIsLit {
		tp := sctx.TableInfo.ColumnConstraints.GetColumnConstraint(lc.Name).Type
		_, err := rv.Value.CastAs(tp)
		if err != nil {
			return errors.Errorf("invalid input syntax for type %s: %s", tp, rh)
		}

		return nil
	}

	if leftIsLit && rightIsCol {
		tp := sctx.TableInfo.ColumnConstraints.GetColumnConstraint(rc.Name).Type
		_, err := lv.Value.CastAs(tp)
		if err != nil {
			return errors.Errorf("invalid input syntax for type %s: %s", tp, lh)
		}

		return nil
	}

	return nil
}

// RemoveUnnecessaryFilterNodesRule removes any filter node whose
// condition is a constant expression that evaluates to a truthy value.
// if it evaluates to a falsy value, it considers that the tree
// will not stream any object, so it returns an empty tree.
func RemoveUnnecessaryFilterNodesRule(sctx *StreamContext) error {
	for i, f := range sctx.Filters {
		switch t := f.Expr.(type) {
		case expr.LiteralValue:
			// Constant expression
			// ex: WHERE 1

			// if the expr is falsy, we return an empty tree
			ok, err := types.IsTruthy(t.Value)
			if err != nil {
				return err
			}
			if !ok {
				sctx.Stream = new(stream.Stream)
				return nil
			}

			// if the expr is truthy, we remove the node from the stream
			sctx.removeFilterNodeByIndex(i)
		}
	}

	return nil
}

// RemoveUnnecessaryProjection removes any project node whose
// expression is a wildcard only.
func RemoveUnnecessaryProjection(sctx *StreamContext) error {
	for i, p := range sctx.Projections {
		if len(p.Exprs) == 1 {
			if _, ok := p.Exprs[0].(expr.Wildcard); ok {
				sctx.removeProjectionNode(i)
			}
		}
	}

	return nil
}

// RemoveUnnecessaryTempSortNodesRule removes any duplicate TempSort node.
// For each stream, there can be at most two TempSort nodes.
// In the following case, we can remove the second TempSort node.
//
//	SELECT * FROM foo GROUP BY a ORDER BY a
//	table.Scan('foo') | docs.TempSort(a) | docs.GroupBy(a) | docs.TempSort(a)
//
// This only works if both temp sort nodes use the same path
func RemoveUnnecessaryTempSortNodesRule(sctx *StreamContext) error {
	if len(sctx.TempTreeSorts) > 2 {
		panic("unexpected number of TempSort nodes")
	}

	if len(sctx.TempTreeSorts) <= 1 {
		return nil
	}

	lcol, ok := sctx.TempTreeSorts[0].Expr.(*expr.Column)
	if !ok {
		return nil
	}

	rcol, ok := sctx.TempTreeSorts[1].Expr.(*expr.Column)
	if !ok {
		return nil
	}

	if lcol.Name != rcol.Name {
		return nil
	}

	// we remove the rightmost one
	// and we override the direction of the first one
	sctx.TempTreeSorts[0].Desc = sctx.TempTreeSorts[1].Desc
	sctx.removeTempTreeNodeNode(sctx.TempTreeSorts[1])

	return nil
}
