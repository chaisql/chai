package planner

import (
	"github.com/genjidb/genji/database"
	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/index"
	"github.com/genjidb/genji/sql/query/expr"
	"github.com/genjidb/genji/sql/scanner"
)

var optimizerRules = []func(t *Tree) (*Tree, error){
	SplitANDConditionRule,
	PrecalculateExprRule,
	RemoveUnnecessarySelectionNodesRule,
	UseIndexBasedOnSelectionNodeRule,
}

// Optimize takes a tree, applies a list of optimization rules
// and returns an optimized tree.
// Depending on the rule, the tree may be modified in place or
// replaced by a new one.
func Optimize(t *Tree) (*Tree, error) {
	var err error

	for _, rule := range optimizerRules {
		t, err = rule(t)
		if err != nil {
			return nil, err
		}
	}

	return t, nil
}

// SplitANDConditionRule splits any selection node whose condition
// is one or more AND operators into one or more selection nodes.
// The condition won't be split if the expression tree contains an OR
// operation.
// Example:
//   this:
//     σ(a > 2 AND b != 3 AND c < 2)
//   becomes this:
//     σ(a > 2)
//     σ(b != 3)
//     σ(c < 2)
func SplitANDConditionRule(t *Tree) (*Tree, error) {
	n := t.Root
	var prev Node

	for n != nil {
		if n.Operation() == Selection {
			sn := n.(*selectionNode)
			cond := sn.cond
			if cond != nil {
				// The AND operator has one of the lowest precedence,
				// only OR has a lower precedence,
				// which means that if AND is used without OR, it will be at
				// the top of the expression tree.
				if op, ok := cond.(expr.Operator); ok && expr.IsAndOperator(op) {
					exprs := splitANDExpr(cond)

					cur := n.Left()
					i := len(exprs) - 1
					var newNode Node
					for i >= 0 {
						newNode = NewSelectionNode(cur, exprs[i])
						err := newNode.Bind(sn.tx, sn.params)
						if err != nil {
							return nil, err
						}
						cur = newNode

						i--
					}

					if prev != nil {
						prev.SetLeft(newNode)
					} else {
						t.Root = newNode
					}
				}
			}
		}

		prev = n
		n = n.Left()
	}

	return t, nil
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
func PrecalculateExprRule(t *Tree) (*Tree, error) {
	n := t.Root

	for n != nil {
		if n.Operation() == Selection {
			sn := n.(*selectionNode)
			sn.cond = precalculateExpr(sn.cond)
		}

		n = n.Left()
	}

	return t, nil
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
			vb := make(document.ValueBuffer, len(t))
			for i := range t {
				vb[i] = document.Value(t[i].(expr.LiteralValue))
			}

			return expr.ArrayValue(&vb)
		}

	case expr.KVPairs:
		// we assume that the list of kvpairs contains only literals
		// until proven wrong.
		literalsOnly := true

		for i, kv := range t {
			kv.V = precalculateExpr(kv.V)
			if _, ok := kv.V.(expr.LiteralValue); !ok {
				literalsOnly = false
			}
			t[i] = kv
		}

		// if literalsOnly is still true, it means we have a list of kvpairs
		// that only contain constant values (ex: {"a": 1, "b": true}.
		// We can transform that into a document.Document.
		if literalsOnly {
			var fb document.FieldBuffer
			for i := range t {
				fb.Add(t[i].K, document.Value(t[i].V.(expr.LiteralValue)))
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
			v, err := t.Eval(expr.EvalStack{})
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

// RemoveUnnecessarySelectionNodesRule removes any selection node whose
// condition is a constant expression that evaluates to a truthy value.
// if it evaluates to a falsy value, it considers that the tree
// will not stream any document, so it returns an empty tree.
func RemoveUnnecessarySelectionNodesRule(t *Tree) (*Tree, error) {
	n := t.Root
	var prev Node

	for n != nil {
		if n.Operation() == Selection {
			sn := n.(*selectionNode)
			if sn.cond != nil {
				if lit, ok := sn.cond.(expr.LiteralValue); ok {
					// if the expr is falsy, we return an empty tree
					ok, err := document.Value(lit).IsTruthy()
					if err != nil {
						return nil, err
					}
					if !ok {
						return &Tree{}, nil
					}
					// if the expr is truthy, we remove the node from the tree
					if prev != nil {
						prev.SetLeft(n.Left())
					} else {
						t.Root = n.Left()
					}
				}

			}
		}

		prev = n
		n = n.Left()
	}

	return t, nil
}

// UseIndexBasedOnSelectionNodeRule scans the tree for the first selection node whose condition is an
// operator that satisfies the following criterias:
// - implements the indexIteratorOperator interface
// - one of its operands is a field selector that is indexed
// - the other operand is a literal value or a parameter
// If found, it will replace the input node by an indexInputNode using this index.
func UseIndexBasedOnSelectionNodeRule(t *Tree) (*Tree, error) {
	n := t.Root
	var prev Node
	var inputNode Node

	// first we lookup for the input node
	for n != nil {
		if n.Operation() == Input {
			inputNode = n
			break
		}

		n = n.Left()
	}

	if inputNode == nil {
		return t, nil
	}

	// then we get the table indexes. here we will assume that at this point
	// inputNodes can only be instances of tableInputNode.
	inpn := inputNode.(*tableInputNode)
	indexes, err := inpn.table.Indexes()
	if err != nil {
		return nil, err
	}

	type candidate struct {
		prevNode, nextNode Node
		in                 *indexInputNode
	}

	var candidates []candidate

	n = t.Root
	// look for all selection nodes that satisfy our requirements
	for n != nil {
		if n.Operation() == Selection {
			sn := n.(*selectionNode)
			indexedNode := selectionNodeValidForIndex(sn, inpn.tableName, indexes)
			if indexedNode != nil {
				candidates = append(candidates, candidate{
					prevNode: prev,
					nextNode: n.Left(),
					in:       indexedNode,
				})
			}
		}

		prev = n
		n = n.Left()
	}

	// determine which index is the most interesting and replace it in the tree.
	// we will assume that unique indexes are more interesting than list indexes
	// because they usually have less elements.
	var selectedCandidate *candidate

	for i, candidate := range candidates {
		if selectedCandidate == nil {
			selectedCandidate = &candidates[i]
			continue
		}

		// if the candidate's related index is a unique index,
		// select it.
		idx := candidate.in.index.(database.Index)
		if _, ok := idx.Index.(*index.UniqueIndex); ok {
			selectedCandidate = &candidates[i]
		}
	}

	if selectedCandidate == nil {
		return t, nil
	}

	// we make sure the new IndexInputNode is bound
	if err := selectedCandidate.in.Bind(inpn.tx, inpn.params); err != nil {
		return nil, err
	}

	// we remove the selection node from the tree
	if selectedCandidate.prevNode == nil {
		t.Root = selectedCandidate.nextNode
	} else {
		selectedCandidate.prevNode.SetLeft(selectedCandidate.nextNode)
	}

	n = t.Root
	prev = nil
	// we lookup again for the input node and the node that is right before.
	for n != nil {
		if n.Operation() == Input {
			break
		}

		prev = n
		n = n.Left()
	}

	// we replace the table input node by the selected indexInputNode
	if prev == nil {
		t.Root = selectedCandidate.in
	} else {
		prev.SetLeft(selectedCandidate.in)
	}

	return t, nil
}

func selectionNodeValidForIndex(sn *selectionNode, tableName string, indexes map[string]database.Index) *indexInputNode {
	if sn.cond == nil {
		return nil
	}

	// the root of the condition must be an operator
	op, ok := sn.cond.(expr.Operator)
	if !ok {
		return nil
	}

	iop, ok := op.(IndexIteratorOperator)
	if !ok {
		return nil
	}

	// determine if the operator can benefit from an index
	ok, field, e := opCanUseIndex(op)
	if !ok {
		return nil
	}

	// analyse the other operand to make sure it's a literal or a param
	if !isLiteralOrParam(e) {
		return nil
	}

	// now, we look if an index exists for that field
	idx, ok := indexes[field.Name()]
	if !ok {
		return nil
	}

	in := NewIndexInputNode(tableName, idx.Opts.IndexName, iop, e, scanner.ASC).(*indexInputNode)
	in.index = idx

	return in
}

func opCanUseIndex(op expr.Operator) (bool, expr.FieldSelector, expr.Expr) {
	lf, leftIsField := op.LeftHand().(expr.FieldSelector)
	rf, rightIsField := op.RightHand().(expr.FieldSelector)

	// field OP expr
	if leftIsField && !rightIsField {
		return true, lf, op.RightHand()
	}

	// expr OP field
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
