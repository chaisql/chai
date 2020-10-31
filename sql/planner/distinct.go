package planner

import (
	"github.com/genjidb/genji/database"
	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/sql/query/expr"
)

type dedupNode struct {
	node

	tableName string
	indexes   map[string]database.Index
}

func NewDedupNode(n Node, tableName string) Node {
	return &dedupNode{
		node: node{
			op:   Dedup,
			left: n,
		},
		tableName: tableName,
	}
}

func (n *dedupNode) Bind(tx *database.Transaction, params []expr.Param) (err error) {
	table, err := tx.GetTable(n.tableName)
	if err != nil {
		return
	}

	n.indexes, err = table.Indexes()
	return
}

func (n *dedupNode) toStream(st document.Stream) (document.Stream, error) {
	set := newDocumentHashSet(nil) // use default hashing algorithm
	return st.Filter(set.Filter), nil
}

func (n *dedupNode) String() string {
	return "Dedup()"
}
