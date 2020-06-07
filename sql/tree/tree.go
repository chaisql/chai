// Package tree provides types to describe the lifecycle of a query.
// Each tree represents a stream of documents that gets transformed by operations,
// following rules of relational algebra.
package tree

import (
	"fmt"

	"github.com/genjidb/genji/database"
	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/sql/query"
	"github.com/genjidb/genji/sql/query/expr"
)

// An Operation can manipulate and transform a stream of data.
type Operation int

const (
	// Input is a node from where data is read. It represents a stream of documents.
	Input Operation = iota
	// Selection (σ) is an operation that filters documents that satisfy a given condition.
	Selection
	// Projection (∏) is an operation that selects a list of fields from each document of a stream.
	Projection
	// Rename (ρ) is an operation that renames a field from each document of a stream.
	Rename
	// Deletion is an operation that removes all of the documents of a stream from their respective table.
	Deletion
	// Replacement is an operation that stores every document of a stream in their respective keys.
	Replacement
	// Limit is an operation that only allows a certain number of documents to be processed
	// by the stream.
	Limit
	// Skip is an operation that ignores a certain number of documents.
	Skip
	// Sort is an operation that sorts a stream of document according to a given field and a direction.
	Sort
	// Set is an operation that adds or replaces a field for every document of the stream.
	Set
	// Unset is an operation that removes a field from every document of a stream
)

// A Tree describes the flow of a stream of documents.
// Each node will manipulate the stream using relational algebra operations.
type Tree struct {
	Root Node
}

// New creates a new tree with n as root.
func New(n Node) *Tree {
	return &Tree{Root: n}
}

// A Node represents an operation on the stream.
type Node interface {
	Operation() Operation
	Left() Node
	Right() Node
	Bind(tx *database.Transaction, params []expr.Param) error
}

type inputNode interface {
	Node

	buildStream() (document.Stream, error)
}

type operationNode interface {
	Node

	toStream(st document.Stream) (document.Stream, error)
}

type outputNode interface {
	Node

	toResult(st document.Stream) (query.Result, error)
}

type node struct {
	op          Operation
	left, right Node
}

func (n *node) Operation() Operation {
	return n.op
}

func (n *node) Left() Node {
	return n.left
}

func (n *node) Right() Node {
	return n.right
}

type selectionNode struct {
	node

	cond   expr.Expr
	tx     *database.Transaction
	params []expr.Param
}

var _ operationNode = (*selectionNode)(nil)

// NewSelectionNode creates a node that filters documents of a stream, according to
// the expression condition.
func NewSelectionNode(n Node, cond expr.Expr) Node {
	return &selectionNode{
		node: node{
			op:   Selection,
			left: n,
		},
		cond: cond,
	}
}

func (n *selectionNode) Bind(tx *database.Transaction, params []expr.Param) (err error) {
	n.tx = tx
	n.params = params
	return
}

func (n *selectionNode) toStream(st document.Stream) (document.Stream, error) {
	if n.cond == nil {
		return st, nil
	}

	stack := expr.EvalStack{
		Tx:     n.tx,
		Params: n.params,
	}

	return st.Filter(func(d document.Document) (bool, error) {
		stack.Document = d
		v, err := n.cond.Eval(stack)
		if err != nil {
			return false, err
		}

		return v.IsTruthy(), nil
	}), nil
}

type limitNode struct {
	node

	limitExpr expr.Expr
	tx        *database.Transaction
	params    []expr.Param
}

var _ operationNode = (*limitNode)(nil)

// NewLimitNode creates a node that limits the number of documents processed by the stream.
func NewLimitNode(n Node, limitExpr expr.Expr) Node {
	return &limitNode{
		node: node{
			op:   Limit,
			left: n,
		},
		limitExpr: limitExpr,
	}
}

func (n *limitNode) Bind(tx *database.Transaction, params []expr.Param) (err error) {
	n.tx = tx
	n.params = params
	return
}

func (n *limitNode) toStream(st document.Stream) (document.Stream, error) {
	stack := expr.EvalStack{
		Tx:     n.tx,
		Params: n.params,
	}

	v, err := n.limitExpr.Eval(stack)
	if err != nil {
		return st, err
	}

	if !v.Type.IsNumber() {
		return st, fmt.Errorf("limit expression must evaluate to a number, got %q", v.Type)
	}

	limit, err := v.ConvertToInt64()
	if err != nil {
		return st, err
	}

	return st.Limit(int(limit)), nil
}

type offsetNode struct {
	node
	offsetExpr expr.Expr

	tx     *database.Transaction
	params []expr.Param
}

var _ operationNode = (*offsetNode)(nil)

// NewOffsetNode creates a node that skips a certain number of documents from the stream.
func NewOffsetNode(n Node, skipExpr expr.Expr) Node {
	return &offsetNode{
		node: node{
			op:   Limit,
			left: n,
		},
		offsetExpr: skipExpr,
	}
}

func (n *offsetNode) Bind(tx *database.Transaction, params []expr.Param) (err error) {
	n.tx = tx
	n.params = params
	return
}

func (n *offsetNode) toStream(st document.Stream) (document.Stream, error) {
	stack := expr.EvalStack{
		Tx:     n.tx,
		Params: n.params,
	}

	v, err := n.offsetExpr.Eval(stack)
	if err != nil {
		return st, err
	}

	if !v.Type.IsNumber() {
		return st, fmt.Errorf("offset expression must evaluate to a number, got %q", v.Type)
	}

	offset, err := v.ConvertToInt64()
	if err != nil {
		return st, err
	}

	return st.Offset(int(offset)), nil
}

type setNode struct {
	node

	field string
	e     expr.Expr

	tx     *database.Transaction
	params []expr.Param
}

var _ operationNode = (*setNode)(nil)

// NewSetNode creates a node that adds or replaces a field for every document of the stream.
func NewSetNode(n Node, field string, e expr.Expr) Node {
	return &setNode{
		node: node{
			op:   Set,
			left: n,
		},
		field: field,
		e:     e,
	}
}

func (n *setNode) Bind(tx *database.Transaction, params []expr.Param) (err error) {
	n.tx = tx
	n.params = params
	return
}

func (n *setNode) toStream(st document.Stream) (document.Stream, error) {
	var fb document.FieldBuffer

	stack := expr.EvalStack{
		Tx:     n.tx,
		Params: n.params,
	}

	return st.Map(func(d document.Document) (document.Document, error) {
		stack.Document = d
		ev, err := n.e.Eval(stack)
		if err != nil && err != document.ErrFieldNotFound {
			return nil, err
		}

		fb.Reset()

		err = fb.ScanDocument(d)
		if err != nil {
			return nil, err
		}

		_, err = fb.GetByField(n.field)

		switch err {
		case nil:
			// If no error, it means that the field already exists
			// and it should be replaced.
			_ = fb.Replace(n.field, ev)
		case document.ErrFieldNotFound:
			// If the field doesn't exist,
			// it should be added to the document.
			fb.Set(n.field, ev)
		}

		return &fb, nil
	}), nil
}

type unsetNode struct {
	node

	field string
}

var _ operationNode = (*unsetNode)(nil)

// NewUnsetNode creates a node that adds or replaces a field for every document of the stream.
func NewUnsetNode(n Node, field string) Node {
	return &unsetNode{
		node: node{
			op:   Set,
			left: n,
		},
		field: field,
	}
}

func (n *unsetNode) Bind(tx *database.Transaction, params []expr.Param) error {
	return nil
}

func (n *unsetNode) toStream(st document.Stream) (document.Stream, error) {
	var fb document.FieldBuffer

	return st.Map(func(d document.Document) (document.Document, error) {
		fb.Reset()

		_, err := d.GetByField(n.field)
		if err != nil {
			if err != document.ErrFieldNotFound {
				return nil, err
			}

			return d, nil
		}

		err = fb.ScanDocument(d)
		if err != nil {
			return nil, err
		}

		err = fb.Delete(n.field)
		if err != nil {
			return nil, err
		}

		return &fb, nil
	}), nil
}
