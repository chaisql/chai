package planner

import (
	"fmt"

	"github.com/genjidb/genji/database"
	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/expr"
	"github.com/genjidb/genji/stream"
)

var optimizerRules = []func(s *stream.Stream, tx *database.Transaction) (*stream.Stream, error){
	SplitANDConditionRule,
	PrecalculateExprRule,
	RemoveUnnecessaryFilterNodesRule,
	RemoveUnnecessaryDistinctNodeRule,
	RemoveUnnecessaryProjection,
	UseIndexBasedOnFilterNodeRule,
}

// Optimize takes a tree, applies a list of optimization rules
// and returns an optimized tree.
// Depending on the rule, the tree may be modified in place or
// replaced by a new one.
func Optimize(s *stream.Stream, tx *database.Transaction) (*stream.Stream, error) {
	var err error

	for _, rule := range optimizerRules {
		s, err = rule(s, tx)
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
//     filter(a > 2 AND b != 3 AND c < 2)
//   becomes this:
//     filter(a > 2)
//     filter(b != 3)
//     filter(c < 2)
func SplitANDConditionRule(s *stream.Stream, _ *database.Transaction) (*stream.Stream, error) {
	n := s.Op

	for n != nil {
		if f, ok := n.(*stream.FilterOperator); ok {
			cond := f.E
			if cond != nil {
				// The AND operator has one of the lowest precedence,
				// only OR has a lower precedence,
				// which means that if AND is used without OR, it will be at
				// the top of the expression tree.
				if op, ok := cond.(expr.Operator); ok && expr.IsAndOperator(op) {
					exprs := splitANDExpr(cond)

					cur := n.GetPrev()
					s.Remove(n)

					for _, e := range exprs {
						cur = stream.InsertAfter(cur, stream.Filter(e))
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
	if ok && expr.IsAndOperator(op) {
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
func PrecalculateExprRule(s *stream.Stream, _ *database.Transaction) (*stream.Stream, error) {
	n := s.Op

	for n != nil {
		switch t := n.(type) {
		case *stream.FilterOperator:
			t.E = precalculateExpr(t.E)
		case *stream.ProjectOperator:
			for i, e := range t.Exprs {
				t.Exprs[i] = precalculateExpr(e)
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
func precalculateExpr(e expr.Expr) expr.Expr {
	switch t := e.(type) {
	case expr.LiteralExprList:
		// we assume that the list of expressions contains only literals
		// until proven wrong.
		literalsOnly := true
		for i, te := range t {
			newExpr := precalculateExpr(te)
			if _, ok := newExpr.(expr.LiteralValue); !ok {
				literalsOnly = false
			}
			t[i] = newExpr
		}

		// if literalsOnly is still true, it means we have a list of constant expressions
		// (ex: [1, 4, true]). We can transform that into a document.Array.
		if literalsOnly {
			values := make([]document.Value, len(t))
			for i := range t {
				values[i] = document.Value(t[i].(expr.LiteralValue))
			}

			return expr.ArrayValue(document.NewValueBuffer(values...))
		}

	case *expr.KVPairs:
		// we assume that the list of kvpairs contains only literals
		// until proven wrong.
		literalsOnly := true

		for i, kv := range t.Pairs {
			kv.V = precalculateExpr(kv.V)
			if _, ok := kv.V.(expr.LiteralValue); !ok {
				literalsOnly = false
			}
			t.Pairs[i] = kv
		}

		// if literalsOnly is still true, it means we have a list of kvpairs
		// that only contain constant values (ex: {"a": 1, "b": true}.
		// We can transform that into a document.Document.
		if literalsOnly {
			var fb document.FieldBuffer
			for i := range t.Pairs {
				fb.Add(t.Pairs[i].K, document.Value(t.Pairs[i].V.(expr.LiteralValue)))
			}

			return expr.LiteralValue(document.NewDocumentValue(&fb))
		}
	case expr.Operator:
		// since expr.Operator is an interface,
		// this optimization must only be applied to
		// a few selected operators that we know about.
		if !expr.IsAndOperator(t) &&
			!expr.IsOrOperator(t) &&
			!expr.IsArithmeticOperator(t) &&
			!expr.IsComparisonOperator(t) {
			return e
		}

		lh := precalculateExpr(t.LeftHand())
		rh := precalculateExpr(t.RightHand())
		t.SetLeftHandExpr(lh)
		t.SetRightHandExpr(rh)

		_, leftIsLit := lh.(expr.LiteralValue)
		_, rightIsLit := rh.(expr.LiteralValue)
		// if both operands are literals, we can precalculate them now
		if leftIsLit && rightIsLit {
			v, err := t.Eval(&expr.Environment{})
			// any error encountered here is unexpected
			if err != nil {
				panic(err)
			}
			// we replace this expression with the result of its evaluation
			return expr.LiteralValue(v)
		}
	}

	return e
}

// RemoveUnnecessaryFilterNodesRule removes any filter node whose
// condition is a constant expression that evaluates to a truthy value.
// if it evaluates to a falsy value, it considers that the tree
// will not stream any document, so it returns an empty tree.
func RemoveUnnecessaryFilterNodesRule(s *stream.Stream, _ *database.Transaction) (*stream.Stream, error) {
	n := s.Op

	for n != nil {
		if f, ok := n.(*stream.FilterOperator); ok {
			if f.E != nil {
				switch t := f.E.(type) {
				case expr.LiteralValue:
					// Constant expression
					// ex: WHERE 1

					// if the expr is falsy, we return an empty tree
					ok, err := document.Value(t).IsTruthy()
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
					if ok && lv.Type == document.ArrayValue {
						l, err := document.ArrayLength(lv.V.(document.Array))
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
func RemoveUnnecessaryProjection(s *stream.Stream, _ *database.Transaction) (*stream.Stream, error) {
	n := s.Op

	for n != nil {
		if p, ok := n.(*stream.ProjectOperator); ok {
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

// RemoveUnnecessaryDistinctNodeRule removes any Dedup nodes
// where projection is already unique.
func RemoveUnnecessaryDistinctNodeRule(s *stream.Stream, tx *database.Transaction) (*stream.Stream, error) {
	n := s.Op

	// we assume that if we are reading from a table, the first
	// operator of the stream has to be a SeqScanOperator
	firstNode := s.First()
	if firstNode == nil {
		return s, nil
	}
	st, ok := firstNode.(*stream.SeqScanOperator)
	if !ok {
		return s, nil
	}

	t, err := tx.GetTable(st.TableName)
	if err != nil {
		return nil, err
	}
	info := t.Info()
	indexes := t.Indexes()

	// this optimization applies to project operators that immediately follow distinct
	for n != nil {
		if d, ok := n.(*stream.DistinctOperator); ok {
			prev := d.GetPrev()
			if prev != nil {
				pn, ok := prev.(*stream.ProjectOperator)
				if ok {

					// if the projection is unique, we remove the node from the tree
					if isProjectionUnique(indexes, pn, info.GetPrimaryKey()) {
						s.Remove(n)
						n = prev
						continue
					}
				}
			}
		}

		n = n.GetPrev()
	}

	return s, nil
}

func isProjectionUnique(indexes database.Indexes, po *stream.ProjectOperator, pk *database.FieldConstraint) bool {
	for _, field := range po.Exprs {
		e, ok := field.(*expr.NamedExpr)
		if ok {
			field = e.Expr
			return false
		}

		switch v := field.(type) {
		case expr.Path:
			if pk != nil && pk.Path.IsEqual(document.Path(v)) {
				continue
			}

			if idx := indexes.GetIndexByPath(document.Path(v)); idx != nil && idx.Info.Unique {
				continue
			}
		case *expr.PKFunc:
			continue
		}

		return false // if one field is not unique, so projection is not unique too.
	}

	return true
}

// UseIndexBasedOnFilterNodeRule scans the tree for the first filter node whose condition is an
// operator that satisfies the following criterias:
// - is a comparison operator
// - one of its operands is a path expression that is indexed
// - the other operand is a literal value or a parameter
// If found, it will replace the input node by an indexInputNode using this index.
// TODO(asdine): add support for ORDER BY and primary keys
func UseIndexBasedOnFilterNodeRule(s *stream.Stream, tx *database.Transaction) (*stream.Stream, error) {
	n := s.Op

	// first we lookup for the seq scan node.
	// Here we will assume that at this point
	// if there is one it has to be the
	// first node of the stream.
	firstNode := s.First()
	if firstNode == nil {
		return s, nil
	}
	st, ok := firstNode.(*stream.SeqScanOperator)
	if !ok {
		return s, nil
	}
	t, err := tx.GetTable(st.TableName)
	if err != nil {
		return nil, err
	}

	indexes := t.Indexes()

	type candidate struct {
		filterOp *stream.FilterOperator
		in       *stream.IndexScanOperator
		index    *database.Index
	}

	var candidates []candidate

	// look for all selection nodes that satisfy our requirements
	for n != nil {
		if f, ok := n.(*stream.FilterOperator); ok {
			indexedNode, idx, err := filterNodeValidForIndex(f, st.TableName, indexes)
			if err != nil {
				return nil, err
			}
			if indexedNode != nil {
				candidates = append(candidates, candidate{
					filterOp: f,
					in:       indexedNode,
					index:    idx,
				})
			}
		}

		n = n.GetPrev()
	}

	// determine which index is the most interesting and replace it in the tree.
	// we will assume that unique indexes are more interesting than list indexes
	// because they usually have less elements.
	var selectedCandidate *candidate
	var cost int

	for i, candidate := range candidates {
		currentCost := candidate.in.Ranges.Cost()

		if selectedCandidate == nil {
			selectedCandidate = &candidates[i]
			cost = currentCost
			continue
		}

		if currentCost < cost {
			selectedCandidate = &candidates[i]
			cost = currentCost
		}

		// if the cost is the same and the candidate's related index is a unique index,
		// select it.
		if currentCost == cost && candidate.index.Info.Unique {
			selectedCandidate = &candidates[i]
		}
	}

	if selectedCandidate == nil {
		return s, nil
	}

	// remove the selection node from the tree
	s.Remove(selectedCandidate.filterOp)

	// we replace the seq scan node by the selected index scan node
	stream.InsertBefore(s.First(), selectedCandidate.in)

	s.Remove(s.First().GetNext())

	return s, nil
}

func filterNodeValidForIndex(sn *stream.FilterOperator, tableName string, indexes database.Indexes) (*stream.IndexScanOperator, *database.Index, error) {
	if sn.E == nil {
		return nil, nil, nil
	}

	// the root of the condition must be an operator
	op, ok := sn.E.(expr.Operator)
	if !ok {
		return nil, nil, nil
	}

	// determine if the operator can read from the index
	if !expr.OperatorIsIndexCompatible(op) {
		return nil, nil, nil
	}

	// determine if the operator can benefit from an index
	ok, path, e := opCanUseIndex(op)
	if !ok {
		return nil, nil, nil
	}

	// analyse the other operand to make sure it's a literal or a param
	if !isLiteralOrParam(e) {
		return nil, nil, nil
	}

	// now, we look if an index exists for that path
	idx := indexes.GetIndexByPath(document.Path(path))
	if idx == nil {
		return nil, nil, nil
	}

	var ranges []stream.Range

	switch op.(type) {
	case *expr.EqOperator:
		ranges = append(ranges, stream.Range{
			Min:   e,
			Exact: true,
		})
	case *expr.GtOperator:
		ranges = append(ranges, stream.Range{
			Min:       e,
			Exclusive: true,
		})
	case *expr.GteOperator:
		ranges = append(ranges, stream.Range{
			Min: e,
		})
	case *expr.LtOperator:
		ranges = append(ranges, stream.Range{
			Max:       e,
			Exclusive: true,
		})
	case *expr.LteOperator:
		ranges = append(ranges, stream.Range{
			Max: e,
		})
	case *expr.InOperator:
		// opCanUseIndex made sure e is an array.
		a := e.(expr.LiteralValue).V.(document.Array)
		err := a.Iterate(func(i int, value document.Value) error {
			ranges = append(ranges, stream.Range{
				Min:   expr.LiteralValue(value),
				Exact: true,
			})
			return nil
		})
		if err != nil {
			return nil, nil, err
		}
	default:
		panic(fmt.Sprintf("unknown operator %#v", op))
	}

	node := stream.IndexScan(idx.Info.IndexName)
	node.Ranges = ranges

	return node, idx, nil
}

func opCanUseIndex(op expr.Operator) (bool, expr.Path, expr.Expr) {
	lf, leftIsField := op.LeftHand().(expr.Path)
	rf, rightIsField := op.RightHand().(expr.Path)

	// Special case for IN operator: only left operand is valid for index usage
	// valid:   a IN [1, 2, 3]
	// invalid: 1 IN a
	if expr.IsInOperator(op) {
		if leftIsField && !rightIsField {
			rh := op.RightHand()
			// The IN operator can use indexes only if the right hand side is an array with constants.
			// At this point, we know that PrecalculateExprRule has converted any constant expression into
			// actual values, so we can check if the right hand side is an array.
			lv, ok := rh.(expr.LiteralValue)
			if !ok || lv.Type != document.ArrayValue {
				return false, nil, nil
			}

			return true, lf, rh
		}

		return false, nil, nil
	}

	// path OP expr
	if leftIsField && !rightIsField {
		return true, lf, op.RightHand()
	}

	// expr OP path
	if rightIsField && !leftIsField {
		return true, rf, op.LeftHand()
	}

	return false, nil, nil
}

func isLiteralOrParam(e expr.Expr) (ok bool) {
	switch e.(type) {
	case expr.LiteralValue, expr.NamedParam, expr.PositionalParam:
		return true
	}

	return false
}
