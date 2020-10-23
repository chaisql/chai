package planner

import (
	"github.com/genjidb/genji/database"
	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/sql/query/expr"
)

type dedupNode struct {
	node
}

func NewDistinctNode(n Node) Node {
	return &dedupNode{
		node{
			op:   Dedup,
			left: n,
		},
	}
}

func (n *dedupNode) Bind(tx *database.Transaction, params []expr.Param) error {
	return nil
}

func (n *dedupNode) toStream(st document.Stream) (document.Stream, error) {
	set := newDocumentHashSet(nil) // use default hashing algorithm
	return st.Filter(set.Filter), nil
}

func (n *dedupNode) String() string {
	return "Dedup()"
}
