// Package planner provides types to describe and manage the lifecycle of a query.
// A query is represented as a tree, which itself represents a stream of documents.
// Each node of the tree is an operation that transforms that stream, following rules
// of relational algebra.
// Once a tree is created, it can be optimized by a list of rules.
package planner

import (
	"fmt"

	"github.com/genjidb/genji/database"
	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/sql/query"
	"github.com/genjidb/genji/sql/query/expr"
)

// An Operation can manipulate and transform a stream of documents.
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
	Unset
)

// A Tree describes the flow of a stream of documents.
// Each node will manipulate the stream using relational algebra operations.
type Tree struct {
	Root Node
}

// NewTree creates a new tree with n as root.
func NewTree(n Node) *Tree {
	return &Tree{Root: n}
}

// Run implements the query.Statement interface.
// It binds the tree to the database resources and executes it.
func (t *Tree) Run(tx *database.Transaction, params []expr.Param) (query.Result, error) {
	err := Bind(t, tx, params)
	if err != nil {
		return query.Result{}, err
	}

	t, err = Optimize(t)
	if err != nil {
		return query.Result{}, err
	}

	return t.execute()
}

func (t *Tree) execute() (query.Result, error) {
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

func (t *Tree) String() string {
	n := t.Root

	if n == nil {
		return ""
	}

	return nodeToString(t.Root)
}

func nodeToString(n Node) string {
	var s string

	if n.Left() != nil {
		s = nodeToString(n.Left())
	}

	if s == "" {
		return fmt.Sprintf("%v", n)
	}

	return fmt.Sprintf("%s -> %v", s, n)
}

// IsReadOnly implements the query.Statement interface.
func (t *Tree) IsReadOnly() bool {
	return false
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

// A Node represents an operation on the stream.
type Node interface {
	Operation() Operation
	Left() Node
	Right() Node
	SetLeft(Node)
	SetRight(Node)
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

func (n *node) SetLeft(ln Node) {
	n.left = ln
}

func (n *node) SetRight(rn Node) {
	n.right = rn
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

		ok, err := v.IsTruthy()
		if err != nil {
			return false, err
		}
		return ok, nil
	}), nil
}

func (n *selectionNode) String() string {
	return fmt.Sprintf("σ(cond: %s)", n.cond)
}

type limitNode struct {
	node

	limit  int
	tx     *database.Transaction
	params []expr.Param
}

var _ operationNode = (*limitNode)(nil)

// NewLimitNode creates a node that limits the number of documents processed by the stream.
func NewLimitNode(n Node, limit int) Node {
	return &limitNode{
		node: node{
			op:   Limit,
			left: n,
		},
		limit: limit,
	}
}

func (n *limitNode) Bind(tx *database.Transaction, params []expr.Param) (err error) {
	n.tx = tx
	n.params = params
	return
}

func (n *limitNode) toStream(st document.Stream) (document.Stream, error) {
	return st.Limit(n.limit), nil
}

func (n *limitNode) String() string {
	return fmt.Sprintf("Limit(%d)", n.limit)
}

type offsetNode struct {
	node
	offset int

	tx     *database.Transaction
	params []expr.Param
}

var _ operationNode = (*offsetNode)(nil)

// NewOffsetNode creates a node that skips a certain number of documents from the stream.
func NewOffsetNode(n Node, offset int) Node {
	return &offsetNode{
		node: node{
			op:   Limit,
			left: n,
		},
		offset: offset,
	}
}

func (n *offsetNode) String() string {
	return fmt.Sprintf("Offset(%d)", n.offset)
}

func (n *offsetNode) Bind(tx *database.Transaction, params []expr.Param) (err error) {
	n.tx = tx
	n.params = params
	return
}

func (n *offsetNode) toStream(st document.Stream) (document.Stream, error) {
	return st.Offset(n.offset), nil
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

func (n *setNode) String() string {
	return fmt.Sprintf("Set(%s = %s)", n.field, n.e)
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

		path := document.NewValuePath(n.field)
		err = fb.Set(path, ev)
		
		return &fb, err
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
			op:   Unset,
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

func (n *unsetNode) String() string {
	return fmt.Sprintf("Unset(%s)", n.field)
}
