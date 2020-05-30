// Package tree provides types to describe the lifecycle of a query.
// Each tree represents a stream of documents that gets transformed by operations,
// following rules of relational algebra.
package tree

import (
	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/sql/query"
	"github.com/genjidb/genji/sql/query/expr"
	"github.com/genjidb/genji/sql/scanner"
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

type inputNode struct {
	node

	inputType string
	inputName string
}

// NewInputNode creates a node that can be used to read documents.
// It describes the kind of input, which can be either a table or an index.
func NewInputNode(inputType, inputName string) Node {
	return &inputNode{
		node: node{
			op: Input,
		},
		inputType: inputType,
		inputName: inputName,
	}
}

type selectionNode struct {
	node

	cond expr.Expr
}

// NewSelectionNode creates a node that filters documents of a stream, according to
// the condition expression.
func NewSelectionNode(n Node, cond expr.Expr) Node {
	return &selectionNode{
		node: node{
			op:   Selection,
			left: n,
		},
		cond: cond,
	}
}

type projectionNode struct {
	node

	expressions []query.ResultField
}

// NewProjectionNode creates a node that uses the given expressions to create a new document
// for each document of the stream. Each expression can extract fields from the incoming
// document, call functions, execute arithmetic operations. etc.
func NewProjectionNode(n Node, expressions []query.ResultField) Node {
	return &projectionNode{
		node: node{
			op:   Projection,
			left: n,
		},
		expressions: expressions,
	}
}

type renameNode struct {
	node

	field document.ValuePath
	alias string
}

// NewRenameNode creates a node that renames each field from every document of
// a stream into the chosen alias.
func NewRenameNode(n Node, field document.ValuePath, alias string) Node {
	return &renameNode{
		node: node{
			op:   Rename,
			left: n,
		},
		field: field,
		alias: alias,
	}
}

type deletionNode struct {
	node

	tableName string
}

// NewDeletionNode creates a node that delete every document of a stream
// from their respective table.
func NewDeletionNode(n Node, tableName string) Node {
	return &deletionNode{
		node: node{
			op:   Deletion,
			left: n,
		},
		tableName: tableName,
	}
}

type replacementNode struct {
	node

	tableName string
}

// NewReplacementNode creates a node that stores every document of a stream
// in their respective table and primary keys.
func NewReplacementNode(n Node, tableName string) Node {
	return &replacementNode{
		node: node{
			op:   Replacement,
			left: n,
		},
		tableName: tableName,
	}
}

type limitNode struct {
	node

	limitExpr expr.Expr
}

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

type skipNode struct {
	node
	skipExpr expr.Expr
}

// NewSkipNode creates a node that skips a certain number of documents from the stream.
func NewSkipNode(n Node, skipExpr expr.Expr) Node {
	return &skipNode{
		node: node{
			op:   Limit,
			left: n,
		},
		skipExpr: skipExpr,
	}
}

type sortNode struct {
	node

	sortField expr.FieldSelector
	direction scanner.Token
}

// NewSortNode creates a node that sorts a stream according to a given
// document field and a sort direction.
func NewSortNode(n Node, sortField expr.FieldSelector, direction scanner.Token) Node {
	return &sortNode{
		node: node{
			op:   Sort,
			left: n,
		},
		sortField: sortField,
		direction: direction,
	}
}

type setNode struct {
	node

	field document.ValuePath
	e     expr.Expr
}

// NewSetNode creates a node that adds or replaces a field for every document of the stream.
func NewSetNode(n Node, field document.ValuePath, e expr.Expr) Node {
	return &setNode{
		node: node{
			op:   Set,
			left: n,
		},
		field: field,
		e:     e,
	}
}

type unsetNode struct {
	node

	field document.ValuePath
}

// NewUnsetNode creates a node that adds or replaces a field for every document of the stream.
func NewUnsetNode(n Node, field document.ValuePath) Node {
	return &unsetNode{
		node: node{
			op:   Set,
			left: n,
		},
		field: field,
	}
}
