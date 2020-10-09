package planner

import (
	"context"
	"errors"
	"fmt"

	"github.com/genjidb/genji/database"
	"github.com/genjidb/genji/document"
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

func (n *tableInputNode) Bind(ctx context.Context, tx *database.Transaction, params []expr.Param) (err error) {
	n.tx = tx
	n.params = params
	n.table, err = tx.GetTable(ctx, n.tableName)
	return
}

func (n *tableInputNode) String() string {
	return fmt.Sprintf("Table(%s)", n.tableName)
}

func (n *tableInputNode) buildStream(ctx context.Context) (document.Stream, error) {
	return document.NewStream(n.table.Iterator(ctx)), nil
}

type indexInputNode struct {
	node

	tableName string
	indexName string

	tx               *database.Transaction
	params           []expr.Param
	table            *database.Table
	index            *database.Index
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

func (n *indexInputNode) Bind(ctx context.Context, tx *database.Transaction, params []expr.Param) (err error) {
	if n.table == nil {
		n.table, err = tx.GetTable(ctx, n.tableName)
		if err != nil {
			return
		}
	}

	if n.index == nil {
		n.index, err = tx.GetIndex(ctx, n.indexName)
		if err != nil {
			return
		}
	}

	n.tx = tx
	n.params = params
	return
}

func (n *indexInputNode) buildStream(ctx context.Context) (document.Stream, error) {
	return document.NewStream(&indexIterator{
		ctx:    ctx,
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
	IterateIndex(ctx context.Context, idx *database.Index, tb *database.Table, v document.Value, fn func(d document.Document) error) error
}

type indexIterator struct {
	ctx              context.Context
	tx               *database.Transaction
	tb               *database.Table
	params           []expr.Param
	index            *database.Index
	iop              IndexIteratorOperator
	e                expr.Expr
	orderByDirection scanner.Token
}

var errStop = errors.New("stop")

func (it indexIterator) Iterate(fn func(d document.Document) error) error {
	if it.e == nil {
		var err error

		if it.orderByDirection == scanner.DESC {
			err = it.index.DescendLessOrEqual(it.ctx, document.Value{}, func(val, key []byte, isEqual bool) error {
				d, err := it.tb.GetDocument(it.ctx, key)
				if err != nil {
					return err
				}

				return fn(d)
			})
		} else {
			err = it.index.AscendGreaterOrEqual(it.ctx, document.Value{}, func(val, key []byte, isEqual bool) error {
				d, err := it.tb.GetDocument(it.ctx, key)
				if err != nil {
					return err
				}

				return fn(d)
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

	return it.iop.IterateIndex(it.ctx, it.index, it.tb, v, fn)
}
