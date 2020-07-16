package query

import (
	"container/heap"
	"errors"

	"github.com/genjidb/genji/database"
	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/document/encoding"
	"github.com/genjidb/genji/engine"
	"github.com/genjidb/genji/index"
	"github.com/genjidb/genji/pkg/bytesutil"
	"github.com/genjidb/genji/sql/query/expr"
	"github.com/genjidb/genji/sql/scanner"
)

type queryPlan struct {
	scanTable bool
	field     *queryPlanField
	sorted    bool
}

type queryPlanField struct {
	indexedField expr.FieldSelector
	op           expr.Operator
	e            expr.Expr
	uniqueIndex  bool
	isPrimaryKey bool
}

func newQueryOptimizer(tx *database.Transaction, tableName string) (qo queryOptimizer, err error) {
	t, err := tx.GetTable(tableName)
	if err != nil {
		return
	}

	indexes, err := t.Indexes()
	if err != nil {
		return
	}

	info, err := t.Info()
	if err != nil {
		return
	}

	return queryOptimizer{
		tx:        tx,
		t:         t,
		tableName: tableName,
		info:      info,
		indexes:   indexes,
	}, nil
}

// queryOptimizer is a really dumb query optimizer. gotta start somewhere. please don't be mad at me.
type queryOptimizer struct {
	tx               *database.Transaction
	t                *database.Table
	tableName        string
	whereExpr        expr.Expr
	args             []expr.Param
	info             *database.TableInfo
	indexes          map[string]database.Index
	orderBy          expr.FieldSelector
	orderByDirection scanner.Token
	limit            int
	offset           int
}

func (qo *queryOptimizer) optimizeQuery() (st document.Stream, err error) {
	qp := qo.buildQueryPlan()

	switch {
	case qp.scanTable:
		st = document.NewStream(qo.t)
	case qp.field.isPrimaryKey:
		if qp.field.e == nil {
			pkit := pkIterator{
				tx:               qo.tx,
				tb:               qo.t,
				info:             qo.info,
				args:             qo.args,
				e:                qp.field.e,
				orderByDirection: qo.orderByDirection,
			}

			if qp.field.op != nil {
				pkit.pkop, _ = qp.field.op.(pkIteratorOperator)
			}

			st = document.NewStream(pkit)
			break
		}

		var v document.Value
		v, err = qp.field.e.Eval(expr.EvalStack{
			Tx:     qo.tx,
			Params: qo.args,
		})
		if err != nil {
			return
		}

		// for all operators except IN, we require the exact same type
		// otherwise we operate a scan table.
		if !expr.IsInOperator(qp.field.op) {
			v, err = v.ConvertTo(qo.info.GetPrimaryKey().Type)
			if err != nil {
				err = nil
				st = document.NewStream(qo.t)
				break
			}
		}

		pkit := pkIterator{
			tx:               qo.tx,
			tb:               qo.t,
			info:             qo.info,
			args:             qo.args,
			e:                qp.field.e,
			orderByDirection: qo.orderByDirection,
			evalValue:        v,
		}

		if qp.field.op != nil {
			pkit.pkop, _ = qp.field.op.(pkIteratorOperator)
		}

		st = document.NewStream(pkit)
	default:
		idxit := indexIterator{
			tx:               qo.tx,
			tb:               qo.t,
			args:             qo.args,
			e:                qp.field.e,
			index:            qo.indexes[qp.field.indexedField.Name()],
			orderByDirection: qo.orderByDirection,
		}

		if qp.field.op != nil {
			idxit.iop, _ = qp.field.op.(indexIteratorOperator)
		}

		st = document.NewStream(idxit)
	}

	st = st.Filter(whereClause(qo.whereExpr, expr.EvalStack{
		Tx:     qo.tx,
		Params: qo.args,
	}))

	if len(qo.orderBy) != 0 && !qp.sorted {
		st, err = qo.sortIterator(st)
	}

	return
}

func (qo *queryOptimizer) buildQueryPlan() queryPlan {
	var qp queryPlan

	qp.field = qo.analyseExpr(qo.whereExpr)
	if qp.field == nil {
		if len(qo.orderBy) != 0 {
			_, ok := qo.indexes[qo.orderBy.Name()]
			pk := qo.info.GetPrimaryKey()
			if ok || (pk != nil && pk.Path.String() == qo.orderBy.Name()) {
				qp.field = &queryPlanField{
					indexedField: qo.orderBy,
					isPrimaryKey: pk != nil && pk.Path.String() == qo.orderBy.Name(),
				}
				qp.sorted = true

				return qp
			}
		}

		qp.scanTable = true
	}

	return qp
}

type indexIteratorOperator interface {
	IterateIndex(idx index.Index, tb *database.Table, v document.Value, fn func(d document.Document) error) error
}

// analyseExpr is a recursive function that scans each node the e Expr tree.
// If it contains a comparison operator, it checks if this operator and its operands
// can benefit from using an index. This check is done in the cmpOpCanUseIndex function.
// If it contains an AND operator it checks if one of the operands can use an index.
func (qo *queryOptimizer) analyseExpr(e expr.Expr) *queryPlanField {
	op, ok := e.(expr.Operator)
	if !ok {
		return nil
	}

	if _, ok := op.(indexIteratorOperator); ok {
		ok, fs, e := cmpOpCanUseIndex(op)
		if !ok {
			return nil
		}
		ok, e = evaluatesToScalarOrParam(e)
		if !ok {
			return nil
		}

		idx, ok := qo.indexes[fs.Name()]
		if ok {
			return &queryPlanField{
				indexedField: fs,
				op:           op,
				e:            e,
				uniqueIndex:  idx.Unique,
			}
		}

		pk := qo.info.GetPrimaryKey()
		if pk != nil && pk.Path.String() == fs.Name() {
			return &queryPlanField{
				indexedField: fs,
				op:           op,
				e:            e,
				uniqueIndex:  true,
				isPrimaryKey: true,
			}
		}

		return nil
	}

	if expr.IsAndOperator(op) {
		nodeL := qo.analyseExpr(op.LeftHand())
		nodeR := qo.analyseExpr(op.LeftHand())

		if nodeL == nil && nodeR == nil {
			return nil
		}

		if nodeL != nil && nodeL.uniqueIndex {
			return nodeL
		}

		if nodeR != nil && nodeR.uniqueIndex {
			return nodeR
		}

		return nodeL
	}

	return nil
}

func cmpOpCanUseIndex(op expr.Operator) (bool, expr.FieldSelector, expr.Expr) {
	lf, leftIsField := op.LeftHand().(expr.FieldSelector)
	rf, rightIsField := op.RightHand().(expr.FieldSelector)

	// field OP expr
	if leftIsField && !rightIsField {
		return true, lf, op.RightHand()
	}

	// expr OP field
	if rightIsField && !leftIsField {
		return true, rf, op.LeftHand()
	}

	return false, nil, nil
}

func evaluatesToScalarOrParam(e expr.Expr) (ok bool, newExpr expr.Expr) {
	switch e.(type) {
	case expr.LiteralValue:
		return true, e
	case expr.NamedParam, expr.PositionalParam:
		return true, e
	case expr.LiteralExprList:
		v, err := e.Eval(expr.EvalStack{})
		if err != nil {
			return false, e
		}

		a, err := v.ConvertToArray()
		if err != nil {
			return false, e
		}

		return true, expr.ArrayValue(a)
	}

	return false, e
}

type indexIterator struct {
	tx               *database.Transaction
	tb               *database.Table
	args             []expr.Param
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
		Params: it.args,
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

type pkIteratorOperator interface {
	IteratePK(tb *database.Table, v document.Value, pkType document.ValueType, fn func(d document.Document) error) error
}

type pkIterator struct {
	tx               *database.Transaction
	tb               *database.Table
	info             *database.TableInfo
	args             []expr.Param
	e                expr.Expr
	pkop             pkIteratorOperator
	orderByDirection scanner.Token
	evalValue        document.Value
}

func (it pkIterator) Iterate(fn func(d document.Document) error) error {
	if it.e == nil {
		var err error

		var d encoding.EncodedDocument
		it := it.tb.Store.NewIterator(engine.IteratorConfig{Reverse: it.orderByDirection == scanner.DESC})
		defer func() {
			it.Close()
		}()

		for it.Seek(nil); it.Valid(); it.Next() {
			d, err = it.Item().ValueCopy(d)
			if err != nil {
				return err
			}
			err = fn(&d)
			if err != nil {
				return err
			}
		}

		return nil
	}

	return it.pkop.IteratePK(it.tb, it.evalValue, it.info.GetPrimaryKey().Type, fn)
}

// sortIterator operates a partial sort on the iterator using a heap.
// This ensures a O(n+k log n) time complexity
// with k being the limit of the query, or the sum of the limit + offset, when both offset and limit are used.
// if there are no limit or offsets, k = n, the number of elements in the table.
// If the sorting is in ascending order, a min-heap will be used
// otherwise a max-heap will be used instead.
// Once the heap is filled entirely with the content of the table a stream is returned.
// During iteration, the stream will pop the k-smallest or k-largest elements, depending on
// the chosen sorting order (ASC or DESC).
// This function is not memory efficient as it's loading the entire table in memory before
// returning the k-smallest or k-largest elements.
func (qo *queryOptimizer) sortIterator(it document.Iterator) (st document.Stream, err error) {
	k := 0
	if qo.limit != -1 {
		k += qo.limit
		if qo.offset != -1 {
			k += qo.offset
		}
	}

	path := document.ValuePath(qo.orderBy)

	var h heap.Interface
	if qo.orderByDirection == scanner.ASC {
		h = new(minHeap)
	} else {
		h = new(maxHeap)
	}

	heap.Init(h)

	err = it.Iterate(func(d document.Document) error {
		v, err := path.GetValue(d)
		if err != nil && err != document.ErrFieldNotFound {
			return err
		}
		if err == document.ErrFieldNotFound {
			v = document.NewNullValue()
		}

		value, err := index.EncodeFieldToIndexValue(v)
		if err != nil {
			return err
		}

		data, err := encoding.EncodeDocument(d)
		if err != nil {
			return err
		}

		heap.Push(h, heapNode{
			value: value,
			data:  data,
		})

		return nil
	})
	if err != nil {
		return
	}

	st = document.NewStream(&sortedIterator{h, k})

	return
}

type sortedIterator struct {
	h heap.Interface
	k int
}

func (s *sortedIterator) Iterate(fn func(d document.Document) error) error {
	i := 0
	for s.h.Len() > 0 && (s.k == 0 || i < s.k) {
		err := fn(encoding.EncodedDocument(heap.Pop(s.h).(heapNode).data))
		if err != nil {
			return err
		}
		i++
	}

	return nil
}

type heapNode struct {
	value []byte
	data  []byte
}

type minHeap []heapNode

func (h minHeap) Len() int           { return len(h) }
func (h minHeap) Less(i, j int) bool { return bytesutil.CompareBytes(h[i].value, h[j].value) < 0 }
func (h minHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *minHeap) Push(x interface{}) {
	*h = append(*h, x.(heapNode))
}

func (h *minHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

type maxHeap struct {
	minHeap
}

func (h maxHeap) Less(i, j int) bool {
	return bytesutil.CompareBytes(h.minHeap[i].value, h.minHeap[j].value) > 0
}
