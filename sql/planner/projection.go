package planner

import (
	"errors"
	"fmt"
	"strings"

	"github.com/genjidb/genji/database"
	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/sql/query"
	"github.com/genjidb/genji/sql/query/expr"
)

// A ProjectionNode is a node that uses the given expressions to create a new document
// for each document of the stream. Each expression can extract fields from the incoming
// document, call functions, execute arithmetic operations. etc.
type ProjectionNode struct {
	node

	Expressions []ResultField
	tableName   string

	cfg *database.TableConfig
	tx  *database.Transaction
}

var _ outputNode = (*ProjectionNode)(nil)
var _ operationNode = (*ProjectionNode)(nil)

// NewProjectionNode creates a ProjectionNode.
func NewProjectionNode(n Node, expressions []ResultField, tableName string) Node {
	return &ProjectionNode{
		node: node{
			op:   Projection,
			left: n,
		},
		Expressions: expressions,
		tableName:   tableName,
	}
}

// IsEqual returns true if other is a *ProjectionNode and contains the
// same information.
func (n *ProjectionNode) IsEqual(other Node) bool {
	if !n.node.IsEqual(other) {
		return false
	}

	on := other.(*ProjectionNode)
	if n.tableName != on.tableName {
		return false
	}

	if len(n.Expressions) != len(on.Expressions) {
		return false
	}

	for i := range n.Expressions {
		switch t := n.Expressions[i].(type) {
		case Wildcard:
			if _, ok := on.Expressions[i].(Wildcard); !ok {
				return false
			}
		case ResultFieldExpr:
			rf, ok := on.Expressions[i].(ResultFieldExpr)
			if !ok {
				return false
			}

			if t.ExprName != rf.ExprName {
				return false
			}

			if !expr.Equal(t.Expr, rf.Expr) {
				return false
			}
		}
	}

	return true
}

// Bind database resources to this node.
func (n *ProjectionNode) Bind(tx *database.Transaction, params []expr.Param) (err error) {
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

func (n *ProjectionNode) toStream(st document.Stream) (document.Stream, error) {
	if st.IsEmpty() {
		d := documentMask{
			resultFields: n.Expressions,
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
			resultFields: n.Expressions,
		}, nil
	}), nil
}

func (n *ProjectionNode) toResult(st document.Stream) (res query.Result, err error) {
	st, err = n.toStream(st)
	if err != nil {
		return
	}

	res.Stream = st
	return
}

func (n *ProjectionNode) String() string {
	var b strings.Builder

	for i, ex := range n.Expressions {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(fmt.Sprintf("%v", ex))
	}

	return fmt.Sprintf("∏(%s)", b.String())
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

func (r ResultFieldExpr) String() string {
	return fmt.Sprintf("%s", r.Expr)
}

// A Wildcard is a ResultField that iterates over all the fields of a document.
type Wildcard struct{}

// Name returns the "*" character.
func (w Wildcard) Name() string {
	return "*"
}

func (w Wildcard) String() string {
	return w.Name()
}

// Iterate call the document iterate method.
func (w Wildcard) Iterate(stack expr.EvalStack, fn func(field string, value document.Value) error) error {
	if stack.Document == nil {
		return errors.New("no table specified")
	}

	return stack.Document.Iterate(fn)
}
