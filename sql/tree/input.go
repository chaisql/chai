package tree

import (
	"errors"

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
	n.table, err = tx.GetTable(n.tableName)
	return
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
	iop              indexIteratorOperator
	e                expr.Expr
	orderByDirection scanner.Token
}

var _ inputNode = (*indexInputNode)(nil)

// newIndexInputNode creates a node that can be used to read documents using an index.
func newIndexInputNode(tableName, indexName string, iop indexIteratorOperator, filter expr.Expr, orderByDirection scanner.Token) Node {
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
	n.table, err = tx.GetTable(n.tableName)
	if err != nil {
		return
	}

	n.index, err = tx.GetIndex(n.indexName)
	if err != nil {
		return
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
	}), nil
}

type indexIteratorOperator interface {
	IterateIndex(idx index.Index, tb *database.Table, v document.Value, fn func(d document.Document) error) error
}

type indexIterator struct {
	tx               *database.Transaction
	tb               *database.Table
	params           []expr.Param
	index            index.Index
	iop              indexIteratorOperator
	e                expr.Expr
	orderByDirection scanner.Token
}

var errStop = errors.New("stop")

func (it indexIterator) Iterate(fn func(d document.Document) error) error {
	if it.e == nil {
		var err error

		if it.orderByDirection == scanner.DESC {
			err = it.index.DescendLessOrEqual(nil, func(val document.Value, key []byte) error {
				r, err := it.tb.GetDocument(key)
				if err != nil {
					return err
				}

				return fn(r)
			})
		} else {
			err = it.index.AscendGreaterOrEqual(nil, func(val document.Value, key []byte) error {
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

	if v.Type.IsNumber() {
		v, err = v.ConvertTo(document.Float64Value)
		if err != nil {
			return err
		}
	}

	return it.iop.IterateIndex(it.index, it.tb, v, fn)
}
