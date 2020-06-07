package tree

import (
	"github.com/genjidb/genji/database"
	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/sql/query"
	"github.com/genjidb/genji/sql/query/expr"
)

type projectionNode struct {
	node

	expressions []query.ResultField
	tableName   string
	cfg         *database.TableConfig
	tx          *database.Transaction
}

var _ outputNode = (*projectionNode)(nil)
var _ operationNode = (*projectionNode)(nil)

// NewProjectionNode creates a node that uses the given expressions to create a new document
// for each document of the stream. Each expression can extract fields from the incoming
// document, call functions, execute arithmetic operations. etc.
func NewProjectionNode(n Node, expressions []query.ResultField, tableName string) Node {
	return &projectionNode{
		node: node{
			op:   Projection,
			left: n,
		},
		expressions: expressions,
		tableName:   tableName,
	}
}

func (n *projectionNode) Bind(tx *database.Transaction, params []expr.Param) (err error) {
	n.tx = tx
	if n.tableName == "" {
		return
	}

	table, err := tx.GetTable(n.tableName)
	if err != nil {
		return err
	}

	n.cfg, err = table.Config()
	return
}

func (n *projectionNode) toStream(st document.Stream) (document.Stream, error) {
	if st.IsEmpty() {
		d := documentMask{
			resultFields: n.expressions,
		}
		var fb document.FieldBuffer
		err := fb.ScanDocument(d)
		if err != nil {
			return st, err
		}

		return document.NewStream(document.NewIterator(fb)), nil
	}

	return st.Map(func(d document.Document) (document.Document, error) {
		return documentMask{
			cfg:          n.cfg,
			r:            d,
			resultFields: n.expressions,
		}, nil
	}), nil
}

func (n *projectionNode) toResult(st document.Stream) (res query.Result, err error) {
	st, err = n.toStream(st)
	if err != nil {
		return
	}

	res.Tx = n.tx
	res.Stream = st
	return
}

type documentMask struct {
	cfg          *database.TableConfig
	r            document.Document
	resultFields []query.ResultField
}

var _ document.Document = documentMask{}

func (r documentMask) GetByField(field string) (document.Value, error) {
	for _, rf := range r.resultFields {
		if rf.Name() == field || rf.Name() == "*" {
			return r.r.GetByField(field)
		}
	}

	return document.Value{}, document.ErrFieldNotFound
}

func (r documentMask) Iterate(fn func(field string, value document.Value) error) error {
	stack := expr.EvalStack{
		Document: r.r,
		Cfg:      r.cfg,
	}

	for _, rf := range r.resultFields {
		err := rf.Iterate(stack, fn)
		if err != nil {
			return err
		}
	}

	return nil
}
