package planner

import (
	"errors"
	"fmt"

	"github.com/genjidb/genji/database"
	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/index"
	"github.com/genjidb/genji/sql/query/expr"
	"github.com/genjidb/genji/sql/scanner"
)

type tableInputNode struct {
	node

	tableName string
	table     *database.Table
	tx        *database.Transaction
	params    []expr.Param
}

var _ inputNode = (*tableInputNode)(nil)

// NewTableInputNode creates an input node that can be used to read documents
// from a table.
func NewTableInputNode(tableName string) Node {
	return &tableInputNode{
		node: node{
			op: Input,
		},
		tableName: tableName,
	}
}

func (n *tableInputNode) Bind(tx *database.Transaction, params []expr.Param) (err error) {
	n.tx = tx
	n.params = params
	n.table, err = tx.GetTable(n.tableName)
	return
}

func (n *tableInputNode) String() string {
	return fmt.Sprintf("Table(%s)", n.tableName)
}

func (n *tableInputNode) buildStream() (document.Stream, error) {
	return document.NewStream(n.table), nil
}

type indexInputNode struct {
	node

	tableName string
	indexName string

	tx               *database.Transaction
	params           []expr.Param
	table            *database.Table
	index            index.Index
	iop              IndexIteratorOperator
	e                expr.Expr
	orderByDirection scanner.Token
}

var _ inputNode = (*indexInputNode)(nil)

// NewIndexInputNode creates a node that can be used to read documents using an index.
func NewIndexInputNode(tableName, indexName string, iop IndexIteratorOperator, filter expr.Expr, orderByDirection scanner.Token) Node {
	return &indexInputNode{
		node: node{
			op: Input,
		},
		tableName:        tableName,
		indexName:        indexName,
		iop:              iop,
		e:                filter,
		orderByDirection: orderByDirection,
	}
}

func (n *indexInputNode) Bind(tx *database.Transaction, params []expr.Param) (err error) {
	if n.table == nil {
		n.table, err = tx.GetTable(n.tableName)
		if err != nil {
			return
		}
	}

	if n.index == nil {
		n.index, err = tx.GetIndex(n.indexName)
		if err != nil {
			return
		}
	}

	n.tx = tx
	n.params = params
	return
}

func (n *indexInputNode) buildStream() (document.Stream, error) {
	return document.NewStream(&indexIterator{
		tx:     n.tx,
		tb:     n.table,
		params: n.params,
		index:  n.index,
		e:      n.e,
		iop:    n.iop,
	}), nil
}

func (n *indexInputNode) String() string {
	return fmt.Sprintf("Index(%s)", n.indexName)
}

// IndexIteratorOperator is an operator that can be used
// as an input node.
type IndexIteratorOperator interface {
	IterateIndex(idx index.Index, tb *database.Table, v document.Value, fn func(d document.Document) error) error
}

type indexIterator struct {
	tx               *database.Transaction
	tb               *database.Table
	params           []expr.Param
	index            index.Index
	iop              IndexIteratorOperator
	e                expr.Expr
	orderByDirection scanner.Token
}

var errStop = errors.New("stop")

func (it indexIterator) Iterate(fn func(d document.Document) error) error {
	if it.e == nil {
		var err error

		if it.orderByDirection == scanner.DESC {
			err = it.index.DescendLessOrEqual(document.Value{}, func(val, key []byte, isEqual bool) error {
				r, err := it.tb.GetDocument(key)
				if err != nil {
					return err
				}

				return fn(r)
			})
		} else {
			err = it.index.AscendGreaterOrEqual(document.Value{}, func(val, key []byte, isEqual bool) error {
				r, err := it.tb.GetDocument(key)
				if err != nil {
					return err
				}

				return fn(r)
			})
		}

		return err
	}

	v, err := it.e.Eval(expr.EvalStack{
		Tx:     it.tx,
		Params: it.params,
	})
	if err != nil {
		return err
	}

	return it.iop.IterateIndex(it.index, it.tb, v, fn)
}
