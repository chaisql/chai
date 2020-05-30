// Package tree provides types to describe the lifecycle of a query.
// Each tree represents a stream of documents that gets transformed by operations,
// following rules of relational algebra.
package tree

import (
	"github.com/genjidb/genji/document"
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
)

// A Tree describes the flow of a stream of documents.
// Each node will manipulate the stream using relational algebra operations.
type Tree struct {
	Root Node
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

	expressions []expr.Expr
}

// NewProjectionNode creates a node that uses the given expressions to create a new document
// for each document of the stream. Each expression can extract fields from the incoming
// document, call functions, execute arithmetic operations. etc.
func NewProjectionNode(n Node, expressions []expr.Expr) Node {
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
}

// NewDeletionNode creates a node that delete every document of a stream
// from their respective table.
func NewDeletionNode(n Node) Node {
	return &deletionNode{
		node: node{
			op:   Deletion,
			left: n,
		},
	}
}

type replacementNode struct {
	node
}

// NewReplacementNode creates a node that stores every document of a stream
// in their respective table and primary keys.
func NewReplacementNode(n Node) Node {
	return &replacementNode{
		node: node{
			op:   Replacement,
			left: n,
		},
	}
}

type limitNode struct {
	node
	limit int
}

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

type skipNode struct {
	node
	skip int
}

// NewSkipNode creates a node that skips a certain number of documents from the stream.
func NewSkipNode(n Node, skip int) Node {
	return &skipNode{
		node: node{
			op:   Limit,
			left: n,
		},
		skip: skip,
	}
}
