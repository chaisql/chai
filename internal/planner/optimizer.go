package planner

import (
	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/internal/database"
	"github.com/genjidb/genji/internal/environment"
	"github.com/genjidb/genji/internal/expr"
	"github.com/genjidb/genji/internal/sql/scanner"
	"github.com/genjidb/genji/internal/stream"
	"github.com/genjidb/genji/internal/stream/docs"
	"github.com/genjidb/genji/internal/stream/path"
	"github.com/genjidb/genji/types"
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
func Optimize(s *stream.Stream, catalog *database.Catalog) (*stream.Stream, error) {
	if firstNode, ok := s.First().(*stream.ConcatOperator); ok {
		// If the first operation is a concat, optimize all streams individually.
		for i, st := range firstNode.Streams {
			ss, err := Optimize(st, catalog)
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
			ss, err := Optimize(st, catalog)
			if err != nil {
				return nil, err
			}
			firstNode.Streams[i] = ss
		}

		return s, nil
	}

	return optimize(s, catalog)
}

type StreamContext struct {
	Catalog       *database.Catalog
	Stream        *stream.Stream
	Filters       []*docs.FilterOperator
	Projections   []*docs.ProjectOperator
	TempTreeSorts []*docs.TempTreeSortOperator
}

func NewStreamContext(s *stream.Stream) *StreamContext {
	sctx := StreamContext{
		Stream: s,
	}

	n := s.First()

	prevIsFilter := false

	for n != nil {
		switch t := n.(type) {
		case *docs.FilterOperator:
			if prevIsFilter || len(sctx.Filters) == 0 {
				sctx.Filters = append(sctx.Filters, t)
				prevIsFilter = true
			}
		case *docs.ProjectOperator:
			sctx.Projections = append(sctx.Projections, t)
			prevIsFilter = false
		case *docs.TempTreeSortOperator:
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

func (sctx *StreamContext) removeFilterNode(f *docs.FilterOperator) {
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

func (sctx *StreamContext) removeTempTreeNodeNode(f *docs.TempTreeSortOperator) {
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

func optimize(s *stream.Stream, catalog *database.Catalog) (*stream.Stream, error) {
	sctx := NewStreamContext(s)
	sctx.Catalog = catalog

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
//   this:
//     docs.Filter(a > 2 AND b != 3 AND c < 2)
//   becomes this:
//     docs.Filter(a > 2)
//     docs.Filter(b != 3)
//     docs.Filter(c < 2)
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
				newF := docs.Filter(e)
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
//   3 + 4 --> 7
//   3 + 1 > 10 - a --> 4 > 10 - a
func PrecalculateExprRule(sctx *StreamContext) error {
	n := sctx.Stream.Op
	var err error

	for n != nil {
		switch t := n.(type) {
		case *docs.FilterOperator:
			t.Expr, err = precalculateExpr(t.Expr)
		case *docs.ProjectOperator:
			for i := range t.Exprs {
				t.Exprs[i], err = precalculateExpr(t.Exprs[i])
				if err != nil {
					return err
				}
			}
		case *docs.TempTreeSortOperator:
			t.Expr, err = precalculateExpr(t.Expr)
		case *path.SetOperator:
			t.Expr, err = precalculateExpr(t.Expr)
		case *docs.EmitOperator:
			for i := range t.Exprs {
				t.Exprs[i], err = precalculateExpr(t.Exprs[i])
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

// precalculateExpr is a recursive function that tries to precalculate
// expression nodes when possible.
// it returns a new expression with simplified nodes.
// if no simplification is possible it returns the same expression.
func precalculateExpr(e expr.Expr) (expr.Expr, error) {
	switch t := e.(type) {
	case expr.LiteralExprList:
		// we assume that the list of expressions contains only literals
		// until proven wrong.
		literalsOnly := true
		for i, te := range t {
			newExpr, err := precalculateExpr(te)
			if err != nil {
				return nil, err
			}
			if _, ok := newExpr.(expr.LiteralValue); !ok {
				literalsOnly = false
			}
			t[i] = newExpr
		}

		// if literalsOnly is still true, it means we have a list or expressions
		// that only contain constant values (ex: [1, true]).
		// We can transform that into a types.Array.
		if literalsOnly {
			var vb document.ValueBuffer
			for i := range t {
				vb.Append(t[i].(expr.LiteralValue).Value)
			}

			return expr.LiteralValue{Value: types.NewArrayValue(&vb)}, nil
		}
	case *expr.KVPairs:
		// we assume that the list of kvpairs contains only literals
		// until proven wrong.
		literalsOnly := true

		var err error
		for i, kv := range t.Pairs {
			kv.V, err = precalculateExpr(kv.V)
			if err != nil {
				return nil, err
			}
			if _, ok := kv.V.(expr.LiteralValue); !ok {
				literalsOnly = false
			}
			t.Pairs[i] = kv
		}

		// if literalsOnly is still true, it means we have a list of kvpairs
		// that only contain constant values (ex: {"a": 1, "b": true}.
		// We can transform that into a types.Document.
		if literalsOnly {
			var fb document.FieldBuffer
			for i := range t.Pairs {
				fb.Add(t.Pairs[i].K, types.Value(t.Pairs[i].V.(expr.LiteralValue).Value))
			}

			return expr.LiteralValue{Value: types.NewDocumentValue(&fb)}, nil
		}
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

		lh, err := precalculateExpr(t.LeftHand())
		if err != nil {
			return nil, err
		}
		rh, err := precalculateExpr(t.RightHand())
		if err != nil {
			return nil, err
		}
		t.SetLeftHandExpr(lh)
		t.SetRightHandExpr(rh)

		if b, ok := t.(*expr.BetweenOperator); ok {
			b.X, err = precalculateExpr(b.X)
			if err != nil {
				return nil, err
			}

			if _, isLit := b.X.(expr.LiteralValue); !isLit {
				break
			}
		}

		_, leftIsLit := lh.(expr.LiteralValue)
		_, rightIsLit := rh.(expr.LiteralValue)
		// if both operands are literals, we can precalculate them now
		if leftIsLit && rightIsLit {
			v, err := t.Eval(&environment.Environment{})
			// any error encountered here is unexpected
			if err != nil {
				panic(err)
			}
			// we replace this expression with the result of its evaluation
			return expr.LiteralValue{Value: v}, nil
		}
	}

	return e, nil
}

// RemoveUnnecessaryFilterNodesRule removes any filter node whose
// condition is a constant expression that evaluates to a truthy value.
// if it evaluates to a falsy value, it considers that the tree
// will not stream any document, so it returns an empty tree.
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
		case *expr.InOperator:
			// IN operator with empty array
			// ex: WHERE a IN []
			lv, ok := t.RightHand().(expr.LiteralValue)
			if ok && lv.Value.Type() == types.ArrayValue {
				l, err := document.ArrayLength(types.As[types.Array](lv.Value))
				if err != nil {
					return err
				}
				// if the array is empty, we return an empty stream
				if l == 0 {
					sctx.Stream = new(stream.Stream)
					return nil
				}
			}
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
// 		SELECT * FROM foo GROUP BY a ORDER BY a
//		table.Scan('foo') | docs.TempSort(a) | docs.GroupBy(a) | docs.TempSort(a)
// This only works if both temp sort nodes use the same path
func RemoveUnnecessaryTempSortNodesRule(sctx *StreamContext) error {
	if len(sctx.TempTreeSorts) > 2 {
		panic("unexpected number of TempSort nodes")
	}

	if len(sctx.TempTreeSorts) <= 1 {
		return nil
	}

	lpath, ok := sctx.TempTreeSorts[0].Expr.(expr.Path)
	if !ok {
		return nil
	}

	rpath, ok := sctx.TempTreeSorts[1].Expr.(expr.Path)
	if !ok {
		return nil
	}

	if !lpath.IsEqual(rpath) {
		return nil
	}

	// we remove the rightmost one
	// and we override the direction of the first one
	sctx.TempTreeSorts[0].Desc = sctx.TempTreeSorts[1].Desc
	sctx.removeTempTreeNodeNode(sctx.TempTreeSorts[1])

	return nil
}
