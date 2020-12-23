package planner

import (
	"errors"
	"fmt"
	"strings"

	"github.com/genjidb/genji/database"
	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/sql/query/expr"
)

// A ProjectionNode is a node that uses the given expressions to create a new document
// for each document of the stream. Each expression can extract fields from the incoming
// document, call functions, execute arithmetic operations. etc.
type ProjectionNode struct {
	node

	Expressions []ProjectedField
	tableName   string

	info *database.TableInfo
	tx   *database.Transaction
}

var _ operationNode = (*ProjectionNode)(nil)

// NewProjectionNode creates a ProjectionNode.
func NewProjectionNode(n Node, expressions []ProjectedField, tableName string) Node {
	return &ProjectionNode{
		node: node{
			op:   Projection,
			left: n,
		},
		Expressions: expressions,
		tableName:   tableName,
	}
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

	n.info, err = table.Info()
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

		st = document.NewStream(document.NewIterator(&fb))
	} else {
		var dm documentMask
		st = st.Map(func(d document.Document) (document.Document, error) {
			dm.info = n.info
			dm.d = d
			dm.resultFields = n.Expressions

			return &dm, nil
		})
	}

	return st, nil
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
	info         *database.TableInfo
	d            document.Document
	resultFields []ProjectedField
}

var _ document.Document = documentMask{}

func (d documentMask) GetByField(field string) (v document.Value, err error) {
	for _, rf := range d.resultFields {
		if rf.Name() == field || rf.Name() == "*" {
			v, err = d.d.GetByField(field)
			if err != document.ErrFieldNotFound {
				return
			}

			var env expr.Environment
			if d.d != nil {
				env.SetCurrentValue(document.NewDocumentValue(d.d))
			}
			var found bool
			err = rf.Iterate(&env, func(f string, value document.Value) error {
				if f == field {
					v = value
					found = true
				}
				return nil
			})

			if found || err != nil {
				return
			}
		}
	}

	err = document.ErrFieldNotFound
	return
}

func (d documentMask) Iterate(fn func(field string, value document.Value) error) error {
	var env expr.Environment
	if d.d != nil {
		env.SetCurrentValue(document.NewDocumentValue(d.d))
	}

	for _, rf := range d.resultFields {
		err := rf.Iterate(&env, fn)
		if err != nil {
			return err
		}
	}

	return nil
}

// MarshalJSON implements the json.Marshaler interface.
func (d documentMask) MarshalJSON() ([]byte, error) {
	return document.MarshalJSON(d)
}

// A ProjectedField is a field that will be part of the projected document that will be returned at the end of a Select statement.
type ProjectedField interface {
	Iterate(env *expr.Environment, fn func(field string, value document.Value) error) error
	Name() string
}

// ProjectedExpr turns any expression into a ResultField.
type ProjectedExpr struct {
	expr.Expr

	ExprName string
}

// Name returns the raw expression.
func (r ProjectedExpr) Name() string {
	return r.ExprName
}

// Iterate evaluates Expr and calls fn once with the result.
func (r ProjectedExpr) Iterate(env *expr.Environment, fn func(field string, value document.Value) error) error {
	v, err := r.Expr.Eval(env)
	if err != nil {
		return err
	}

	return fn(r.ExprName, v)
}

func (r ProjectedExpr) String() string {
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
func (w Wildcard) Iterate(env *expr.Environment, fn func(field string, value document.Value) error) error {
	v, ok := env.GetCurrentValue()
	if !ok || v.Type != document.DocumentValue {
		return errors.New("no table specified")
	}

	return v.V.(document.Document).Iterate(fn)
}
