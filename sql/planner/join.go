package planner

import (
	"fmt"
	"github.com/genjidb/genji/database"
	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/sql/query/expr"
)

var _ document.Document = (*joinedDocument)(nil)

type joinedDocument struct {
	leftTable, rightTable string
	L, R                  document.Document
}

func (j joinedDocument) Iterate(fn func(field string, value document.Value) error) error {
	err := j.L.Iterate(fn)
	if err != nil {
		return err
	}

	return j.R.Iterate(fn)
}

func (j joinedDocument) GetByField(field string) (document.Value, error) {
	switch field {
	case j.leftTable:
		return document.NewDocumentValue(j.L), nil
	case j.rightTable:
		return document.NewDocumentValue(j.R), nil
	}

	l, err := j.L.GetByField(field)
	if err != nil && err != document.ErrFieldNotFound {
		return document.Value{}, err
	}
	lerr := err

	r, err := j.R.GetByField(field)
	if err != nil && err != document.ErrFieldNotFound {
		return document.Value{}, err
	}

	switch {
	// Field does exist in both documents
	case err == nil && lerr == nil:
		return document.Value{}, fmt.Errorf("field reference %q is ambiguous", field)
	// Field does not exist in both documents
	case err == document.ErrFieldNotFound && lerr == document.ErrFieldNotFound:
		return document.Value{}, err
	// Field does exist in left document
	case lerr == nil:
		return l, nil
	}

	return r, nil
}

type TableJoin struct {
	Table string
	Cond  expr.Expr
}

type nestedLoopJoinNode struct {
	node

	join  TableJoin
	outer *database.Table

	tx     *database.Transaction
	params []expr.Param
}

func NewJoinNode(n Node, join TableJoin) Node {
	return &nestedLoopJoinNode{
		node: node{
			op:   Join,
			left: n,
		},
		join: join,
	}
}

func (n *nestedLoopJoinNode) String() string {
	return fmt.Sprintf("NestedLoopJoin(INNER JOIN %s ON %v)", n.join.Table, n.join.Cond)
}

func (n *nestedLoopJoinNode) Bind(tx *database.Transaction, params []expr.Param) (err error) {
	n.tx = tx
	n.params = params
	n.outer, err = tx.GetTable(n.join.Table)
	return
}

func (n *nestedLoopJoinNode) toStream(st document.Stream) (document.Stream, error) {
	stack := expr.EvalStack{
		Tx:     n.tx,
		Params: n.params,
	}

	var leftTableName string
	if t, ok := n.left.(*tableInputNode); ok {
		leftTableName = t.tableName
	}

	it := func(fn func(document.Document) error) error {
		return st.Iterate(func(l document.Document) error {
			return n.outer.Iterate(func(r document.Document) error {
				d := joinedDocument{leftTable: leftTableName, rightTable: n.join.Table, L: l, R: r}
				stack.Document = d

				v, err := n.join.Cond.Eval(stack)
				if err != nil {
					return err
				}

				ok, err := v.IsTruthy()
				if err != nil {
					return err
				}

				if ok {
					return fn(d)
				}
				return nil
			})
		})
	}

	return document.NewStream(document.IteratorFunc(it)), nil
}

type TableHashJoin struct {
	LeftTable, RightTable string
	LeftExpression        expr.Expr
	RightExpression       expr.Expr
}

type hashJoinNode struct {
	node

	join  TableHashJoin
	inner *database.Table
	outer *database.Table

	tx     *database.Transaction
	params []expr.Param
}

func NewHashJoinNode(n Node, join TableHashJoin) Node {
	return &hashJoinNode{
		node: node{
			op:   Join,
			left: n,
		},
		join: join,
	}
}

func (n *hashJoinNode) String() string {
	return fmt.Sprintf("HashJoin(%s INNER JOIN %s ON %v = %v)", n.join.LeftTable, n.join.RightTable, n.join.LeftExpression, n.join.RightExpression)
}

func (n *hashJoinNode) Bind(tx *database.Transaction, params []expr.Param) (err error) {
	n.tx = tx
	n.params = params
	n.inner, err = tx.GetTable(n.join.LeftTable)
	if err != nil {
		return
	}
	n.outer, err = tx.GetTable(n.join.RightTable)
	return
}

func (n *hashJoinNode) toStream(st document.Stream) (document.Stream, error) {
	stack := expr.EvalStack{
		Tx:     n.tx,
		Params: n.params,
	}

	m := newDocumentHashMap(nil) // default hash algorithm
	err := n.outer.Iterate(func(d document.Document) error {
		stack.Document = d
		v, err := n.join.RightExpression.Eval(stack)
		if err != nil {
			return err
		}

		return m.Add(v, d)
	})
	if err != nil {
		return document.Stream{}, nil
	}

	it := func(fn func(document.Document) error) error {
		return st.Iterate(func(l document.Document) error {
			stack.Document = l
			v, err := n.join.LeftExpression.Eval(stack)
			if err != nil {
				return err
			}

			r, err := m.Get(v)
			if err != nil {
				if err == document.ErrValueNotFound {
					return nil
				}
				return err
			}

			return fn(joinedDocument{
				leftTable:  n.join.LeftTable,
				rightTable: n.join.RightTable,
				L:          l,
				R:          r,
			})
		})
	}

	return document.NewStream(document.IteratorFunc(it)), nil
}
