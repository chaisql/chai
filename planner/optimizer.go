package planner

import (
	"github.com/genjidb/genji/database"
	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/expr"
	"github.com/genjidb/genji/stream"
	"github.com/genjidb/genji/stringutil"
)

var optimizerRules = []func(s *stream.Stream, tx *database.Transaction, params []expr.Param) (*stream.Stream, error){
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
func Optimize(s *stream.Stream, tx *database.Transaction, params []expr.Param) (*stream.Stream, error) {
	var err error

	for _, rule := range optimizerRules {
		s, err = rule(s, tx, params)
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
func SplitANDConditionRule(s *stream.Stream, _ *database.Transaction, _ []expr.Param) (*stream.Stream, error) {
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
func PrecalculateExprRule(s *stream.Stream, _ *database.Transaction, params []expr.Param) (*stream.Stream, error) {
	n := s.Op

	var err error
	for n != nil {
		switch t := n.(type) {
		case *stream.FilterOperator:
			t.E, err = precalculateExpr(t.E, params)
			if err != nil {
				return nil, err
			}
		case *stream.ProjectOperator:
			for i, e := range t.Exprs {
				t.Exprs[i], err = precalculateExpr(e, params)
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
func precalculateExpr(e expr.Expr, params []expr.Param) (expr.Expr, error) {
	switch t := e.(type) {
	case expr.LiteralExprList:
		// we assume that the list of expressions contains only literals
		// until proven wrong.
		literalsOnly := true
		for i, te := range t {
			newExpr, err := precalculateExpr(te, params)
			if err != nil {
				return nil, err
			}
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

			return expr.ArrayValue(document.NewValueBuffer(values...)), nil
		}

	case *expr.KVPairs:
		// we assume that the list of kvpairs contains only literals
		// until proven wrong.
		literalsOnly := true

		var err error
		for i, kv := range t.Pairs {
			kv.V, err = precalculateExpr(kv.V, params)
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
		// We can transform that into a document.Document.
		if literalsOnly {
			var fb document.FieldBuffer
			for i := range t.Pairs {
				fb.Add(t.Pairs[i].K, document.Value(t.Pairs[i].V.(expr.LiteralValue)))
			}

			return expr.LiteralValue(document.NewDocumentValue(&fb)), nil
		}
	case expr.Operator:
		// since expr.Operator is an interface,
		// this optimization must only be applied to
		// a few selected operators that we know about.
		if !expr.IsAndOperator(t) &&
			!expr.IsOrOperator(t) &&
			!expr.IsArithmeticOperator(t) &&
			!expr.IsComparisonOperator(t) {
			return e, nil
		}

		lh, err := precalculateExpr(t.LeftHand(), params)
		if err != nil {
			return nil, err
		}
		rh, err := precalculateExpr(t.RightHand(), params)
		if err != nil {
			return nil, err
		}
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
			return expr.LiteralValue(v), nil
		}
	case expr.PositionalParam, expr.NamedParam:
		v, err := e.Eval(&expr.Environment{Params: params})
		if err != nil {
			return nil, err
		}
		return expr.LiteralValue(v), nil
	}

	return e, nil
}

// RemoveUnnecessaryFilterNodesRule removes any filter node whose
// condition is a constant expression that evaluates to a truthy value.
// if it evaluates to a falsy value, it considers that the tree
// will not stream any document, so it returns an empty tree.
func RemoveUnnecessaryFilterNodesRule(s *stream.Stream, _ *database.Transaction, _ []expr.Param) (*stream.Stream, error) {
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
func RemoveUnnecessaryProjection(s *stream.Stream, _ *database.Transaction, _ []expr.Param) (*stream.Stream, error) {
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
func RemoveUnnecessaryDistinctNodeRule(s *stream.Stream, tx *database.Transaction, _ []expr.Param) (*stream.Stream, error) {
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
// TODO(asdine): add support for ORDER BY
func UseIndexBasedOnFilterNodeRule(s *stream.Stream, tx *database.Transaction, params []expr.Param) (*stream.Stream, error) {
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

	info := t.Info()
	indexes := t.Indexes()

	var candidates []*candidate

	// look for all selection nodes that satisfy our requirements
	for n != nil {
		if f, ok := n.(*stream.FilterOperator); ok {
			candidate, err := getCandidateFromfilterNode(f, st.TableName, info, indexes)
			if err != nil {
				return nil, err
			}
			if candidate != nil {
				candidates = append(candidates, candidate)
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
		currentCost := candidate.cost

		if selectedCandidate == nil {
			selectedCandidate = candidates[i]
			cost = currentCost
			continue
		}

		if currentCost < cost {
			selectedCandidate = candidates[i]
			cost = currentCost
		}

		// if the cost is the same and the candidate's related index has a higher priority,
		// select it.
		if currentCost == cost && selectedCandidate.priority < candidate.priority {
			selectedCandidate = candidates[i]
		}
	}

	if selectedCandidate == nil {
		return s, nil
	}

	// remove the selection node from the tree
	s.Remove(selectedCandidate.filterOp)

	// we replace the seq scan node by the selected index scan node
	stream.InsertBefore(s.First(), selectedCandidate.newOp)

	s.Remove(s.First().GetNext())

	return s, nil
}

type candidate struct {
	// filter operator to remove and replace by either an indexScan
	// or pkScan operators.
	filterOp *stream.FilterOperator
	// the candidate indexScan or pkScan operator
	newOp stream.Operator
	// the cost of the candidate
	cost int
	// is this candidate reading from an index
	isIndex bool
	// is this candidate reading primary key ranges
	isPk bool
	// if the costs of two candidates are equal,
	// this number determines which node will be prioritized
	priority int
}

// getCandidateFromfilterNode analyses f and determines if it can be replaced by an indexScan or pkScan operator.
func getCandidateFromfilterNode(f *stream.FilterOperator, tableName string, info *database.TableInfo, indexes database.Indexes) (*candidate, error) {
	if f.E == nil {
		return nil, nil
	}

	// the root of the condition must be an operator
	op, ok := f.E.(expr.Operator)
	if !ok {
		return nil, nil
	}

	// determine if the operator can read from the index
	if !expr.OperatorIsIndexCompatible(op) {
		return nil, nil
	}

	// determine if the operator can benefit from an index
	ok, path, e := opCanUseIndex(op)
	if !ok {
		return nil, nil
	}

	// analyse the other operand to make sure it's a literal
	ev, ok := e.(expr.LiteralValue)
	if !ok {
		return nil, nil
	}
	v := document.Value(ev)

	// now, we look if an index exists for that path
	cd := candidate{
		filterOp: f,
	}

	// we'll start with checking if the path is the primary key of the table
	if pk := info.GetPrimaryKey(); pk != nil && pk.Path.IsEqual(path) {
		// if both types are different, don't select this scanner
		if pk.Type != v.Type {
			return nil, nil
		}

		cd.isPk = true
		cd.priority = 3

		ranges, err := getRangesFromOp(op, v)
		if err != nil {
			return nil, err
		}

		cd.newOp = stream.PkScan(tableName, ranges...)
		cd.cost = ranges.Cost()
		return &cd, nil
	}

	// if not, check if an index exists for that path
	if idx := indexes.GetIndexByPath(document.Path(path)); idx != nil {
		// if both types are different, don't select this scanner
		if !idx.Info.Type.IsZero() && idx.Info.Type != v.Type {
			return nil, nil
		}

		cd.isIndex = true
		if idx.Info.Unique {
			cd.priority = 2
		} else {
			cd.priority = 1
		}

		ranges, err := getRangesFromOp(op, v)
		if err != nil {
			return nil, err
		}

		cd.newOp = stream.IndexScan(idx.Info.IndexName, ranges...)
		cd.cost = ranges.Cost()

		return &cd, nil
	}

	return nil, nil
}

func opCanUseIndex(op expr.Operator) (bool, document.Path, expr.Expr) {
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

			return true, document.Path(lf), rh
		}

		return false, nil, nil
	}

	// path OP expr
	if leftIsField && !rightIsField {
		return true, document.Path(lf), op.RightHand()
	}

	// expr OP path
	if rightIsField && !leftIsField {
		return true, document.Path(rf), op.LeftHand()
	}

	return false, nil, nil
}

func getRangesFromOp(op expr.Operator, v document.Value) (stream.Ranges, error) {
	var ranges stream.Ranges

	switch op.(type) {
	case *expr.EqOperator:
		ranges = ranges.Append(stream.Range{
			Min:   v,
			Exact: true,
		})
	case *expr.GtOperator:
		ranges = ranges.Append(stream.Range{
			Min:       v,
			Exclusive: true,
		})
	case *expr.GteOperator:
		ranges = ranges.Append(stream.Range{
			Min: v,
		})
	case *expr.LtOperator:
		ranges = ranges.Append(stream.Range{
			Max:       v,
			Exclusive: true,
		})
	case *expr.LteOperator:
		ranges = ranges.Append(stream.Range{
			Max: v,
		})
	case *expr.InOperator:
		// opCanUseIndex made sure e is an array.
		a := v.V.(document.Array)
		err := a.Iterate(func(i int, value document.Value) error {
			ranges = ranges.Append(stream.Range{
				Min:   value,
				Exact: true,
			})
			return nil
		})
		if err != nil {
			return nil, err
		}
	default:
		panic(stringutil.Sprintf("unknown operator %#v", op))
	}

	return ranges, nil
}
