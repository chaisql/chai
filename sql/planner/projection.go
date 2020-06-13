package planner

import (
	"errors"

	"github.com/genjidb/genji/database"
	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/sql/query"
	"github.com/genjidb/genji/sql/query/expr"
)

type projectionNode struct {
	node

	expressions []ResultField
	tableName   string

	cfg *database.TableConfig
	tx  *database.Transaction
}

var _ outputNode = (*projectionNode)(nil)
var _ operationNode = (*projectionNode)(nil)

// NewProjectionNode creates a node that uses the given expressions to create a new document
// for each document of the stream. Each expression can extract fields from the incoming
// document, call functions, execute arithmetic operations. etc.
func NewProjectionNode(n Node, expressions []ResultField, tableName string) Node {
	return &projectionNode{
		node: node{
			op:   Projection,
			left: n,
		},
		expressions: expressions,
		tableName:   tableName,
	}
}

func (n *projectionNode) Equal(other Node) bool {
	if !n.node.Equal(other) {
		return false
	}

	on := other.(*projectionNode)
	if n.tableName != on.tableName {
		return false
	}

	if len(n.expressions) != len(on.expressions) {
		return false
	}

	for i := range n.expressions {
		switch t := n.expressions[i].(type) {
		case Wildcard:
			if _, ok := on.expressions[i].(query.Wildcard); !ok {
				return false
			}
		case ResultFieldExpr:
			rf, ok := on.expressions[i].(query.ResultFieldExpr)
			if !ok {
				return false
			}

			if t.ExprName != rf.ExprName {
				return false
			}

			if !t.Expr.Equal(rf.Expr) {
				return false
			}
		}
	}

	return true
}

func (n *projectionNode) Bind(tx *database.Transaction, params []expr.Param) (err error) {
	n.tx = tx
	if n.tableName == "" {
		return
	}

	table, err := tx.GetTable(n.tableName)
	if err != nil {
		return err
	}

	n.cfg, err = table.Config()
	return
}

func (n *projectionNode) toStream(st document.Stream) (document.Stream, error) {
	if st.IsEmpty() {
		d := documentMask{
			resultFields: n.expressions,
		}
		var fb document.FieldBuffer
		err := fb.ScanDocument(d)
		if err != nil {
			return st, err
		}

		return document.NewStream(document.NewIterator(fb)), nil
	}

	return st.Map(func(d document.Document) (document.Document, error) {
		return documentMask{
			cfg:          n.cfg,
			r:            d,
			resultFields: n.expressions,
		}, nil
	}), nil
}

func (n *projectionNode) toResult(st document.Stream) (res query.Result, err error) {
	st, err = n.toStream(st)
	if err != nil {
		return
	}

	res.Tx = n.tx
	res.Stream = st
	return
}

type documentMask struct {
	cfg          *database.TableConfig
	r            document.Document
	resultFields []ResultField
}

var _ document.Document = documentMask{}

func (r documentMask) GetByField(field string) (document.Value, error) {
	for _, rf := range r.resultFields {
		if rf.Name() == field || rf.Name() == "*" {
			return r.r.GetByField(field)
		}
	}

	return document.Value{}, document.ErrFieldNotFound
}

func (r documentMask) Iterate(fn func(field string, value document.Value) error) error {
	stack := expr.EvalStack{
		Document: r.r,
		Cfg:      r.cfg,
	}

	for _, rf := range r.resultFields {
		err := rf.Iterate(stack, fn)
		if err != nil {
			return err
		}
	}

	return nil
}

// A ResultField is a field that will be part of the result document that will be returned at the end of a Select statement.
type ResultField interface {
	Iterate(stack expr.EvalStack, fn func(field string, value document.Value) error) error
	Name() string
}

// ResultFieldExpr turns any expression into a ResultField.
type ResultFieldExpr struct {
	expr.Expr

	ExprName string
}

// Name returns the raw expression.
func (r ResultFieldExpr) Name() string {
	return r.ExprName
}

// Iterate evaluates Expr and calls fn once with the result.
func (r ResultFieldExpr) Iterate(stack expr.EvalStack, fn func(field string, value document.Value) error) error {
	v, err := r.Expr.Eval(stack)
	if err != nil {
		return err
	}

	return fn(r.ExprName, v)
}

// A Wildcard is a ResultField that iterates over all the fields of a document.
type Wildcard struct{}

// Name returns the "*" character.
func (w Wildcard) Name() string {
	return "*"
}

// Iterate call the document iterate method.
func (w Wildcard) Iterate(stack expr.EvalStack, fn func(field string, value document.Value) error) error {
	if stack.Document == nil {
		return errors.New("no table specified")
	}

	return stack.Document.Iterate(fn)
}
