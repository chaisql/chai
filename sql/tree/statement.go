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

func treeToResult(t *Tree) (query.Result, error) {
	var st document.Stream
	var err error

	if t.Root.Left() != nil {
		st, err = nodeToStream(t.Root.Left())
		if err != nil {
			return query.Result{}, err
		}
	}

	return t.Root.(outputNode).toResult(st)
}

func nodeToStream(n Node) (st document.Stream, err error) {
	l := n.Left()
	if l != nil {
		st, err = nodeToStream(l)
		if err != nil {
			return
		}
	}

	switch t := n.(type) {
	case inputNode:
		st, err = t.buildStream()
	case operationNode:
		st, err = t.toStream(st)
	default:
		panic(fmt.Sprintf("incorrect node type %#v", n))
	}

	return
}

type treeStatement struct {
	t *Tree
}

func (s treeStatement) Run(tx *database.Transaction, params []expr.Param) (query.Result, error) {
	err := Bind(s.t, tx, params)
	if err != nil {
		return query.Result{}, err
	}

	return treeToResult(s.t)
}

func (s treeStatement) IsReadOnly() bool {
	return false
}
