package parser

import (
	"fmt"

	"github.com/genjidb/genji/document"
)

type constraintNode struct {
	frag   document.ValuePathFragment
	typ    document.ValueType
	parent *constraintNode
	sub    []*constraintNode
}

func createConstraintNode(parent *constraintNode, path document.ValuePath, typ document.ValueType) *constraintNode {
	node := &constraintNode{
		frag:   path[0],
		parent: parent,
	}

	if len(path) == 1 {
		node.typ = typ
		return node
	}

	// Here we check next path fragment
	// If next is index, current node is array
	// Otherwise it is a document
	if path[1].FieldName != "" {
		node.typ = document.DocumentValue
	} else {
		node.typ = document.ArrayValue
	}

	node.sub = append(node.sub, createConstraintNode(node, path[1:], typ))
	return node
}

func (n *constraintNode) getPath() document.ValuePath {
	if n.parent == nil {
		return document.ValuePath{n.frag}
	}

	return append(n.parent.getPath(), n.frag)
}

func (n *constraintNode) search(path document.ValuePath) *constraintNode {
	if n.frag == path[0] {
		if len(path) == 1 {
			return n
		}

		for _, sub := range n.sub {
			t := sub.search(path[1:])
			if t != nil {
				return t
			}
		}
	}

	return nil
}

func (n *constraintNode) insert(path document.ValuePath, typ document.ValueType) error {
	switch {
	case len(path) == 1:
		return fmt.Errorf("%q already exists as type %s", n.getPath().String(), n.typ.String())
	// when type is explicitly set and does not have array or document type
	case n.typ != 0 && n.typ != document.ArrayValue && n.typ != document.DocumentValue:
		p := append(n.getPath(), path[1]).String()
		return fmt.Errorf("%q already exists as type %s, but trying add %q constraint", p, n.getPath().String(), n.typ.String())
	// when constraint tries to set document constraint for path, but there is already a array constraint
	case path[1].FieldName != "" && n.typ == document.ArrayValue:
		p := append(n.getPath(), path[1]).String()
		return fmt.Errorf("%q already exists as array, but trying add %q constraint", p, n.getPath().String())
	// when constraint tries to set array constraint for path, but there is already a document constraint
	case path[1].FieldName == "" && n.typ == document.DocumentValue:
		p := append(n.getPath(), path[1]).String()
		return fmt.Errorf("%q already exists as document, but trying add %q constraint", p, n.getPath().String())
	}

	for _, sub := range n.sub {
		if sub.frag == path[1] {
			return sub.insert(path[1:], typ)
		}
	}

	n.sub = append(n.sub, createConstraintNode(n, path[1:], typ))
	return nil
}

type constraintTree struct {
	roots []*constraintNode
}

func (tree *constraintTree) insert(path document.ValuePath, typ document.ValueType) error {
	for _, sub := range tree.roots {
		if sub.frag == path[0] {
			return sub.insert(path, typ)
		}
	}

	tree.roots = append(tree.roots, createConstraintNode(nil, path, typ))
	return nil
}

func (tree *constraintTree) search(path document.ValuePath) *constraintNode {
	for _, sub := range tree.roots {
		if sub.frag == path[0] {
			return sub.search(path)
		}
	}

	return nil
}
