package query

import (
	"bytes"
	"container/heap"
	"errors"

	"github.com/asdine/genji/database"
	"github.com/asdine/genji/document"
	"github.com/asdine/genji/document/encoding"
	"github.com/asdine/genji/engine"
	"github.com/asdine/genji/index"
	"github.com/asdine/genji/sql/scanner"
)

type queryPlan struct {
	scanTable bool
	field     *queryPlanField
	sorted    bool
}

type queryPlanField struct {
	indexedField FieldSelector
	op           scanner.Token
	e            Expr
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

	cfg, err := t.Config()
	if err != nil {
		return
	}

	return queryOptimizer{
		tx:        tx,
		t:         t,
		tableName: tableName,
		cfg:       cfg,
		indexes:   indexes,
	}, nil
}

// queryOptimizer is a really dumb query optimizer. gotta start somewhere. please don't be mad at me.
type queryOptimizer struct {
	tx               *database.Transaction
	t                *database.Table
	tableName        string
	whereExpr        Expr
	args             []Param
	cfg              *database.TableConfig
	indexes          map[string]database.Index
	orderBy          FieldSelector
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
			st = document.NewStream(pkIterator{
				tx:               qo.tx,
				tb:               qo.t,
				cfg:              qo.cfg,
				args:             qo.args,
				op:               qp.field.op,
				e:                qp.field.e,
				orderByDirection: qo.orderByDirection,
			})
			break
		}

		var v document.Value
		v, err = qp.field.e.Eval(EvalStack{
			Tx:     qo.tx,
			Params: qo.args,
		})
		if err != nil {
			return
		}

		v, err := v.ConvertTo(qo.cfg.GetPrimaryKey().Type)
		if err != nil {
			st = document.NewStream(qo.t)
			break
		}

		st = document.NewStream(pkIterator{
			tx:               qo.tx,
			tb:               qo.t,
			cfg:              qo.cfg,
			args:             qo.args,
			op:               qp.field.op,
			e:                qp.field.e,
			orderByDirection: qo.orderByDirection,
			evalValue:        v,
		})
	default:
		st = document.NewStream(indexIterator{
			tx:               qo.tx,
			tb:               qo.t,
			args:             qo.args,
			op:               qp.field.op,
			e:                qp.field.e,
			index:            qo.indexes[qp.field.indexedField.Name()],
			orderByDirection: qo.orderByDirection,
		})
	}

	st = st.Filter(whereClause(qo.whereExpr, EvalStack{
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
			pk := qo.cfg.GetPrimaryKey()
			if ok || (pk != nil && pk.Path.String() == qo.orderBy.Name()) {
				qp.field = &queryPlanField{
					indexedField: qo.orderBy,
					isPrimaryKey: pk.Path.String() == qo.orderBy.Name(),
				}
				qp.sorted = true

				return qp
			}
		}

		qp.scanTable = true
	}

	return qp
}

// analyseExpr is a recursive function that scans each node the e Expr tree.
// If it contains a comparison operator, it checks if this operator and its operands
// can benefit from using an index. This check is done in the cmpOpCanUseIndex function.
// If it contains an AND operator it checks if one of the operands can use an index.
func (qo *queryOptimizer) analyseExpr(e Expr) *queryPlanField {
	switch t := e.(type) {
	case CmpOp:
		ok, fs, e := cmpOpCanUseIndex(&t)
		if !ok || !evaluatesToScalarOrParam(e) {
			return nil
		}

		idx, ok := qo.indexes[fs.Name()]
		if ok {
			return &queryPlanField{
				indexedField: fs,
				op:           t.Token,
				e:            e,
				uniqueIndex:  idx.Unique,
			}
		}

		pk := qo.cfg.GetPrimaryKey()
		if pk != nil && pk.Path.String() == fs.Name() {
			return &queryPlanField{
				indexedField: fs,
				op:           t.Token,
				e:            e,
				uniqueIndex:  true,
				isPrimaryKey: true,
			}
		}

		return nil

	case *AndOp:
		nodeL := qo.analyseExpr(t.LeftHand())
		nodeR := qo.analyseExpr(t.LeftHand())

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

func cmpOpCanUseIndex(cmp *CmpOp) (bool, FieldSelector, Expr) {
	switch cmp.Token {
	case scanner.EQ, scanner.GT, scanner.GTE, scanner.LT, scanner.LTE:
	default:
		return false, nil, nil
	}

	lf, leftIsField := cmp.LeftHand().(FieldSelector)
	rf, rightIsField := cmp.RightHand().(FieldSelector)

	// field OP expr
	if leftIsField && !rightIsField {
		return true, lf, cmp.RightHand()
	}

	// expr OP field
	if rightIsField && !leftIsField {
		return true, rf, cmp.LeftHand()
	}

	return false, nil, nil
}

func evaluatesToScalarOrParam(e Expr) bool {
	switch e.(type) {
	case LiteralValue:
		return true
	case NamedParam, PositionalParam:
		return true
	}

	return false
}

type indexIterator struct {
	tx               *database.Transaction
	tb               *database.Table
	args             []Param
	index            index.Index
	op               scanner.Token
	e                Expr
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

	v, err := it.e.Eval(EvalStack{
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

	switch it.op {
	case scanner.EQ:
		err = it.index.AscendGreaterOrEqual(&index.Pivot{Value: v}, func(val document.Value, key []byte) error {
			ok, err := v.IsEqual(val)
			if err != nil {
				return err
			}

			if ok {
				r, err := it.tb.GetDocument(key)
				if err != nil {
					return err
				}

				return fn(r)
			}

			return errStop
		})
	case scanner.GT:
		err = it.index.AscendGreaterOrEqual(&index.Pivot{Value: v}, func(val document.Value, key []byte) error {
			ok, err := v.IsEqual(val)
			if err != nil {
				return err
			}

			if ok {
				return nil
			}

			r, err := it.tb.GetDocument(key)
			if err != nil {
				return err
			}

			return fn(r)
		})
	case scanner.GTE:
		err = it.index.AscendGreaterOrEqual(&index.Pivot{Value: v}, func(val document.Value, key []byte) error {
			r, err := it.tb.GetDocument(key)
			if err != nil {
				return err
			}

			return fn(r)
		})
	case scanner.LT:
		err = it.index.AscendGreaterOrEqual(index.EmptyPivot(v.Type), func(val document.Value, key []byte) error {
			ok, err := v.IsLesserThanOrEqual(val)
			if err != nil {
				return err
			}

			if ok {
				return errStop
			}

			r, err := it.tb.GetDocument(key)
			if err != nil {
				return err
			}

			return fn(r)
		})
	case scanner.LTE:
		err = it.index.AscendGreaterOrEqual(index.EmptyPivot(v.Type), func(val document.Value, key []byte) error {
			ok, err := v.IsLesserThan(val)
			if err != nil {
				return err
			}

			if ok {
				return errStop
			}

			r, err := it.tb.GetDocument(key)
			if err != nil {
				return err
			}

			return fn(r)
		})
	}

	if err != nil && err != errStop {
		return err
	}

	return nil
}

type pkIterator struct {
	tx               *database.Transaction
	tb               *database.Table
	cfg              *database.TableConfig
	args             []Param
	op               scanner.Token
	e                Expr
	orderByDirection scanner.Token
	evalValue        document.Value
}

func (it pkIterator) Iterate(fn func(d document.Document) error) error {
	if it.e == nil {
		var err error

		var d encoding.EncodedDocument
		it := it.tb.Store.NewIterator(engine.IteratorConfig{Reverse: it.orderByDirection == scanner.DESC})
		defer it.Close()

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

	data, err := encoding.EncodeValue(it.evalValue)
	if err != nil {
		return err
	}

	switch it.op {
	case scanner.EQ:
		val, err := it.tb.Store.Get(data)
		if err != nil {
			if err == engine.ErrKeyNotFound {
				return nil
			}

			return err
		}
		return fn(encoding.EncodedDocument(val))
	case scanner.GT:
		var d encoding.EncodedDocument
		it := it.tb.Store.NewIterator(engine.IteratorConfig{})
		defer it.Close()

		for it.Seek(data); it.Valid(); it.Next() {
			d, err = it.Item().ValueCopy(d)
			if err != nil {
				return err
			}
			if bytes.Equal(data, d) {
				return nil
			}

			err = fn(&d)
			if err != nil {
				return err
			}
		}
	case scanner.GTE:
		var d encoding.EncodedDocument
		it := it.tb.Store.NewIterator(engine.IteratorConfig{})
		defer it.Close()

		for it.Seek(data); it.Valid(); it.Next() {
			d, err = it.Item().ValueCopy(d)
			if err != nil {
				return err
			}

			err = fn(&d)
			if err != nil {
				return err
			}
		}
	case scanner.LT:
		var d encoding.EncodedDocument
		it := it.tb.Store.NewIterator(engine.IteratorConfig{})
		defer it.Close()

		for it.Seek(nil); it.Valid(); it.Next() {
			d, err = it.Item().ValueCopy(d)
			if err != nil {
				return err
			}
			if bytes.Compare(data, d) <= 0 {
				break
			}

			err = fn(&d)
			if err != nil {
				return err
			}
		}
	case scanner.LTE:
		var d encoding.EncodedDocument
		it := it.tb.Store.NewIterator(engine.IteratorConfig{})
		defer it.Close()

		for it.Seek(nil); it.Valid(); it.Next() {
			d, err = it.Item().ValueCopy(d)
			if err != nil {
				return err
			}
			if bytes.Compare(data, d) < 0 {
				break
			}

			err = fn(&d)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// sortIterator operates a partial sort on the iterator using a heap.
// This ensures a O(n+klog n) time complexity
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
func (h minHeap) Less(i, j int) bool { return bytes.Compare(h[i].value, h[j].value) < 0 }
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
	return bytes.Compare(h.minHeap[i].value, h.minHeap[j].value) > 0
}
