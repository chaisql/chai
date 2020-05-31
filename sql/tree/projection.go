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

func (n *projectionNode) toStream(st document.Stream, stack expr.EvalStack) (document.Stream, expr.EvalStack, error) {
	if st.IsEmpty() {
		d := documentMask{
			resultFields: n.expressions,
		}
		var fb document.FieldBuffer
		err := fb.ScanDocument(d)
		if err != nil {
			return st, stack, err
		}

		return document.NewStream(document.NewIterator(fb)), stack, nil
	}

	return st.Map(func(d document.Document) (document.Document, error) {
		return documentMask{
			cfg:          stack.Cfg,
			r:            d,
			resultFields: n.expressions,
		}, nil
	}), stack, nil
}

func (n *projectionNode) toResult(st document.Stream, stack expr.EvalStack) (res query.Result, err error) {
	st, stack, err = n.toStream(st, stack)
	if err != nil {
		return
	}

	res.Tx = stack.Tx
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
