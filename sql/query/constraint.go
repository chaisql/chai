package query

import (
	"fmt"

	"github.com/genjidb/genji/database"

	"github.com/genjidb/genji/document"
)

// constraintNode is a tree node which stores a type of document field
type constraintNode struct {
	frag   document.ReferenceFragment
	typ    document.ValueType
	parent *constraintNode
	sub    []*constraintNode
}

func createConstraintNode(parent *constraintNode, ref document.Reference, typ document.ValueType) *constraintNode {
	node := &constraintNode{
		frag:   ref[0],
		parent: parent,
	}

	if len(ref) == 1 {
		node.typ = typ
		return node
	}

	// Here we check next ref fragment
	// If next is index, current node is array
	// Otherwise it is a document
	if ref[1].FieldName != "" {
		node.typ = document.DocumentValue
	} else {
		node.typ = document.ArrayValue
	}

	node.sub = append(node.sub, createConstraintNode(node, ref[1:], typ))
	return node
}

func (n *constraintNode) getRef() document.Reference {
	if n.parent == nil {
		return document.Reference{n.frag}
	}

	return append(n.parent.getRef(), n.frag)
}

func (n *constraintNode) search(ref document.Reference) *constraintNode {
	if n.frag != ref[0] {
		return nil
	}

	if len(ref) == 1 {
		return n
	}

	for _, sub := range n.sub {
		t := sub.search(ref[1:])
		if t != nil {
			return t
		}
	}

	return nil
}

func (n *constraintNode) insert(ref document.Reference, typ document.ValueType) error {
	switch {
	case len(ref) == 1:
		return fmt.Errorf("%q already exists as type %s", n.getRef().String(), n.typ.String())
	// when type is explicitly set and does not have array or document type
	case n.typ != 0 && n.typ != document.ArrayValue && n.typ != document.DocumentValue:
		p := append(n.getRef(), ref[1]).String()
		return fmt.Errorf("%q already exists as type %s, but trying add %q constraint", n.getRef().String(), n.typ.String(), p)
	// when constraint tries to set document constraint for ref, but there is already a array constraint
	case ref[1].FieldName != "" && n.typ == document.ArrayValue:
		p := append(n.getRef(), ref[1]).String()
		return fmt.Errorf("%q already exists as array, but trying add %q constraint", n.getRef().String(), p)
	// when constraint tries to set array constraint for ref, but there is already a document constraint
	case ref[1].FieldName == "" && n.typ == document.DocumentValue:
		p := append(n.getRef(), ref[1]).String()
		return fmt.Errorf("%q already exists as document, but trying add %q constraint", n.getRef().String(), p)
	}

	for _, sub := range n.sub {
		if sub.frag == ref[1] {
			return sub.insert(ref[1:], typ)
		}
	}

	n.sub = append(n.sub, createConstraintNode(n, ref[1:], typ))
	return nil
}

// constraintTree is a tree of document field types.
// Field type can be set explicitly or derived from ref.
// Example query:
// 	CREATE TABLE foo(a.b TEXT, a.d[1])
// as a tree
// 	document(a)
// 	├── text(b)
// 	├── array(d)
// 	│   ├──[1] any
// 	│   └──...
//
type constraintTree struct {
	roots []*constraintNode
}

func (tree *constraintTree) insert(ref document.Reference, typ document.ValueType) error {
	for _, sub := range tree.roots {
		if sub.frag == ref[0] {
			return sub.insert(ref, typ)
		}
	}

	tree.roots = append(tree.roots, createConstraintNode(nil, ref, typ))
	return nil
}

func (tree *constraintTree) search(ref document.Reference) *constraintNode {
	for _, sub := range tree.roots {
		if sub.frag == ref[0] {
			return sub.search(ref)
		}
	}

	return nil
}

func checkConstraints(constraints []database.FieldConstraint) error {
	tree := constraintTree{}
	for _, fc := range constraints {
		if err := tree.insert(fc.Reference, fc.Type); err != nil {
			return fmt.Errorf("incoherent field constraint: %w", err)
		}
	}

	return nil
}
