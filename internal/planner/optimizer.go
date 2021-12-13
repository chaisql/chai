package planner

import (
	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/internal/database"
	"github.com/genjidb/genji/internal/environment"
	"github.com/genjidb/genji/internal/expr"
	"github.com/genjidb/genji/internal/sql/scanner"
	"github.com/genjidb/genji/internal/stream"
	"github.com/genjidb/genji/types"
)

var optimizerRules = []func(s *stream.Stream, catalog *database.Catalog) (*stream.Stream, error){
	SplitANDConditionRule,
	PrecalculateExprRule,
	RemoveUnnecessaryProjection,
	RemoveUnnecessaryFilterNodesRule,
	SelectIndex,
}

// Optimize takes a tree, applies a list of optimization rules
// and returns an optimized tree.
// Depending on the rule, the tree may be modified in place or
// replaced by a new one.
func Optimize(s *stream.Stream, catalog *database.Catalog) (*stream.Stream, error) {
	var err error

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

	for _, rule := range optimizerRules {
		s, err = rule(s, catalog)
		if err != nil {
			return nil, err
		}
		if s.Op == nil {
			break
		}
	}

	return s, nil
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
func SplitANDConditionRule(s *stream.Stream, _ *database.Catalog) (*stream.Stream, error) {
	n := s.Op

	for n != nil {
		if f, ok := n.(*stream.DocsFilterOperator); ok {
			cond := f.E
			if cond != nil {
				// The AND operator has one of the lowest precedence,
				// only OR has a lower precedence,
				// which means that if AND is used without OR, it will be at
				// the top of the expression tree.
				if op, ok := cond.(expr.Operator); ok && op.Token() == scanner.AND {
					exprs := splitANDExpr(cond)

					cur := n.GetPrev()
					s.Remove(n)

					for _, e := range exprs {
						cur = stream.InsertAfter(cur, stream.DocsFilter(e))
					}

					if s.Op == nil {
						s.Op = cur
					}
				}
			}
		}

		n = n.GetPrev()
	}

	return s, nil
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
func PrecalculateExprRule(s *stream.Stream, _ *database.Catalog) (*stream.Stream, error) {
	n := s.Op

	var err error
	for n != nil {
		switch t := n.(type) {
		case *stream.DocsFilterOperator:
			t.E, err = precalculateExpr(t.E)
			if err != nil {
				return nil, err
			}
		case *stream.DocsProjectOperator:
			for i, e := range t.Exprs {
				t.Exprs[i], err = precalculateExpr(e)
				if err != nil {
					return nil, err
				}
			}
		}

		n = n.GetPrev()
	}

	return s, nil
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
func RemoveUnnecessaryFilterNodesRule(s *stream.Stream, _ *database.Catalog) (*stream.Stream, error) {
	n := s.Op

	for n != nil {
		if f, ok := n.(*stream.DocsFilterOperator); ok {
			if f.E != nil {
				switch t := f.E.(type) {
				case expr.LiteralValue:
					// Constant expression
					// ex: WHERE 1

					// if the expr is falsy, we return an empty tree
					ok, err := types.IsTruthy(t.Value)
					if err != nil {
						return nil, err
					}
					if !ok {
						return &stream.Stream{}, nil
					}

					// if the expr is truthy, we remove the node from the stream
					prev := n.GetPrev()
					s.Remove(n)
					n = prev
					continue
				case *expr.InOperator:
					// IN operator with empty array
					// ex: WHERE a IN []
					lv, ok := t.RightHand().(expr.LiteralValue)
					if ok && lv.Value.Type() == types.ArrayValue {
						l, err := document.ArrayLength(lv.Value.V().(types.Array))
						if err != nil {
							return nil, err
						}
						// if the array is empty, we return an empty stream
						if l == 0 {
							return &stream.Stream{}, nil
						}
					}
				}
			}
		}

		n = n.GetPrev()
	}

	return s, nil
}

// RemoveUnnecessaryProjection removes any project node whose
// expression is a wildcard only.
func RemoveUnnecessaryProjection(s *stream.Stream, _ *database.Catalog) (*stream.Stream, error) {
	n := s.Op

	for n != nil {
		if p, ok := n.(*stream.DocsProjectOperator); ok {
			if len(p.Exprs) == 1 {
				if _, ok := p.Exprs[0].(expr.Wildcard); ok {
					prev := n.GetPrev()
					s.Remove(n)
					n = prev
				}
			}
		}

		n = n.GetPrev()
	}

	return s, nil
}
