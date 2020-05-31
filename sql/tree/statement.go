package tree

import (
	"fmt"

	"github.com/genjidb/genji/database"
	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/sql/query"
	"github.com/genjidb/genji/sql/query/expr"
)

// NewStatement from a tree.
func NewStatement(t *Tree) query.Statement {
	return &treeStatement{t: t}
}

func treeToResult(t *Tree, stack expr.EvalStack) (query.Result, error) {
	var st document.Stream
	var err error

	if t.Root.Left() != nil {
		st, stack, err = nodeToStream(t.Root.Left(), stack)
		if err != nil {
			return query.Result{}, err
		}
	}

	return t.Root.(outputNode).toResult(st, stack)
}

func nodeToStream(n Node, stack expr.EvalStack) (st document.Stream, newStack expr.EvalStack, err error) {
	l := n.Left()
	if l != nil {
		st, newStack, err = nodeToStream(l, stack)
		if err != nil {
			return
		}
		stack = newStack
	}

	switch t := n.(type) {
	case inputNode:
		st, newStack, err = t.buildStream(stack)
	case operationNode:
		st, newStack, err = t.toStream(st, stack)
	default:
		panic(fmt.Sprintf("incorrect node type %#v", n))
	}

	return
}

type treeStatement struct {
	t *Tree
}

func (s treeStatement) Run(tx *database.Transaction, params []expr.Param) (query.Result, error) {
	stack := expr.EvalStack{
		Tx:     tx,
		Params: params,
	}

	return treeToResult(s.t, stack)
}

func (s treeStatement) IsReadOnly() bool {
	return false
}
