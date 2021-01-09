package stream

import (
	"bytes"
	"container/heap"
	"errors"
	"fmt"

	"github.com/genjidb/genji/database"
	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/sql/query/expr"
)

const (
	groupEnvKey     = "_group"
	groupExprEnvKey = "_group_expr"
	accEnvKey       = "_acc"
)

// ErrInvalidResult is returned when an expression supposed to evaluate to a document
// returns something else.
var ErrInvalidResult = errors.New("expression must evaluate to a document")

// An Operator is used to modify a stream.
// It takes an environment containing the current value as well as any other metadata
// created by other operatorsand returns a new environment which will be passed to the next operator.
// If it returns a nil environment, the env will be ignored.
// If it returns an error, the stream will be interrupted and that error will bubble up
// and returned by this function, unless that error is ErrStreamClosed, in which case
// the Iterate method will stop the iteration and return nil.
// Stream operators can be reused, and thus, any state or side effect should be kept within the Op closure
// unless the nature of the operator prevents that.
type Operator interface {
	Op() (OperatorFunc, error)
}

// An OperatorFunc is the function that will receive each value of the stream.
type OperatorFunc func(env *expr.Environment) (*expr.Environment, error)

// A MapOperator applies an expression on each value of the stream and returns a new value.
type MapOperator struct {
	E expr.Expr
}

// Map evaluates e on each value of the stream and outputs the result.
func Map(e expr.Expr) *MapOperator {
	return &MapOperator{E: e}
}

// Op implements the Operator interface.
func (m *MapOperator) Op() (OperatorFunc, error) {
	var newEnv expr.Environment

	return func(env *expr.Environment) (*expr.Environment, error) {
		v, err := m.E.Eval(env)
		if err != nil {
			return nil, err
		}

		if v.Type != document.DocumentValue {
			return nil, ErrInvalidResult
		}

		newEnv.SetDocument(v.V.(document.Document))
		newEnv.Outer = env
		return &newEnv, nil
	}, nil
}

func (m *MapOperator) String() string {
	return fmt.Sprintf("map(%s)", m.E)
}

// A FilterOperator filters values based on a given expression.
type FilterOperator struct {
	E expr.Expr
}

// Filter evaluates e for each incoming value and filters any value whose result is not truthy.
func Filter(e expr.Expr) *FilterOperator {
	return &FilterOperator{E: e}
}

// Op implements the Operator interface.
func (m *FilterOperator) Op() (OperatorFunc, error) {
	return func(env *expr.Environment) (*expr.Environment, error) {
		v, err := m.E.Eval(env)
		if err != nil {
			return nil, err
		}

		ok, err := v.IsTruthy()
		if err != nil {
			return nil, err
		}

		if !ok {
			return nil, nil
		}

		return env, nil
	}, nil
}

func (m *FilterOperator) String() string {
	return fmt.Sprintf("filter(%s)", m.E)
}

// A TakeOperator closes the stream after a certain number of values.
type TakeOperator struct {
	N int64
}

// Take closes the stream after n values have passed through the operator.
func Take(n int64) *TakeOperator {
	return &TakeOperator{N: n}
}

// Op implements the Operator interface.
func (m *TakeOperator) Op() (OperatorFunc, error) {
	var count int64

	return func(env *expr.Environment) (*expr.Environment, error) {
		if count < m.N {
			count++
			return env, nil
		}

		return nil, ErrStreamClosed
	}, nil
}

func (m *TakeOperator) String() string {
	return fmt.Sprintf("take(%d)", m.N)
}

// A SkipOperator skips the n first values of the stream.
type SkipOperator struct {
	N int64
}

// Skip ignores the first n values of the stream.
func Skip(n int64) *SkipOperator {
	return &SkipOperator{N: n}
}

// Op implements the Operator interface.
func (m *SkipOperator) Op() (OperatorFunc, error) {
	var skipped int64

	return func(env *expr.Environment) (*expr.Environment, error) {
		if skipped < m.N {
			skipped++
			return nil, nil
		}

		return env, nil
	}, nil
}

func (m *SkipOperator) String() string {
	return fmt.Sprintf("skip(%d)", m.N)
}

// A GroupByOperator applies an expression on each value of the stream and stores the result in the _group
// variable in the output stream.
type GroupByOperator struct {
	E expr.Expr
}

// GroupBy applies e on each value of the stream and stores the result in the _group
// variable in the output stream.
func GroupBy(e expr.Expr) *GroupByOperator {
	return &GroupByOperator{E: e}
}

// Op implements the Operator interface.
func (op *GroupByOperator) Op() (OperatorFunc, error) {
	var newEnv expr.Environment

	return func(env *expr.Environment) (*expr.Environment, error) {
		v, err := op.E.Eval(env)
		if err != nil {
			return nil, err
		}

		newEnv.Set(groupEnvKey, v)
		newEnv.Set(groupExprEnvKey, document.NewTextValue(fmt.Sprintf("%s", op.E)))
		newEnv.Outer = env
		return &newEnv, nil
	}, nil
}

func (op *GroupByOperator) String() string {
	return fmt.Sprintf("groupBy(%s)", op.E)
}

// A SortOperator consumes every value of the stream and outputs them in order.
type SortOperator struct {
	Expr expr.Expr
	Desc bool
}

// Sort consumes every value of the stream and outputs them in order.
// It operates a partial sort on the iterator using a heap.
// This ensures a O(k+n log n) time complexity, where k is the sum of
// Take() + Skip() operators, if provided, otherwise k = n.
// If the sorting is in ascending order, a min-heap will be used
// otherwise a max-heap will be used instead.
// Once the heap is filled entirely with the content of the incoming stream, a stream is returned.
// During iteration, the stream will pop the k-smallest or k-largest elements, depending on
// the chosen sorting order (ASC or DESC).
// This function is not memory efficient as it is loading the entire stream in memory before
// returning the k-smallest or k-largest elements.
func Sort(e expr.Expr) *SortOperator {
	return &SortOperator{Expr: e}
}

// SortReverse does the same as Sort but in descending order.
func SortReverse(e expr.Expr) *SortOperator {
	return &SortOperator{Expr: e, Desc: true}
}

// Pipe stores s in the operator and return a new Stream with the reduce operator appended. It implements the Piper interface.
func (op *SortOperator) Pipe(s Stream) Stream {
	return Stream{
		It: IteratorFunc(func(env *expr.Environment, fn func(env *expr.Environment) error) error {
			return op.iterate(s, env, fn)
		}),
	}
}

// Op implements the Operator interface but should never be called by Stream.
func (op *SortOperator) Op() (OperatorFunc, error) {
	return func(env *expr.Environment) (*expr.Environment, error) {
		return env, nil
	}, nil
}

func (op *SortOperator) iterate(s Stream, env *expr.Environment, fn func(env *expr.Environment) error) error {
	h, err := op.sortStream(s, env)
	if err != nil {
		return err
	}

	for h.Len() > 0 {
		node := heap.Pop(h).(heapNode)
		err := fn(node.data)
		if err != nil {
			return err
		}
	}

	return nil
}

func (op *SortOperator) sortStream(st Stream, env *expr.Environment) (heap.Interface, error) {
	var h heap.Interface
	if op.Desc {
		h = new(maxHeap)
	} else {
		h = new(minHeap)
	}

	heap.Init(h)

	return h, st.Iterate(env, func(env *expr.Environment) error {
		sortV, err := op.Expr.Eval(env)
		if err != nil {
			return err
		}

		// We need to make sure sort behaviour
		// is the same with or without indexes.
		// To achieve that, the value must be encoded using the same method
		// as what the index package would do.
		var buf bytes.Buffer

		err = document.NewValueEncoder(&buf).Encode(sortV)
		if err != nil {
			return err
		}

		node := heapNode{
			value: buf.Bytes(),
		}
		node.data, err = env.Clone()
		if err != nil {
			return err
		}

		heap.Push(h, node)

		return nil
	})
}

func (op *SortOperator) String() string {
	return fmt.Sprintf("sort(%s)", op.Expr)
}

type heapNode struct {
	value []byte
	data  *expr.Environment
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

// A TableInsertOperator inserts incoming documents to the table.
type TableInsertOperator struct {
	Name string
}

// TableInsert inserts incoming documents to the table.
func TableInsert(tableName string) *TableInsertOperator {
	return &TableInsertOperator{Name: tableName}
}

// Op implements the Operator interface.
func (op *TableInsertOperator) Op() (OperatorFunc, error) {
	var newEnv expr.Environment

	var table *database.Table
	return func(env *expr.Environment) (*expr.Environment, error) {
		d, ok := env.GetDocument()
		if !ok {
			return nil, errors.New("missing document")
		}

		if table == nil {
			var err error
			table, err = env.GetTx().GetTable(op.Name)
			if err != nil {
				return nil, err
			}
		}

		d, err := table.Insert(d)
		if err != nil {
			return nil, err
		}

		newEnv.SetDocument(d)
		newEnv.Outer = env
		return &newEnv, nil
	}, nil
}

func (op *TableInsertOperator) String() string {
	return fmt.Sprintf("tableInsert('%s')", op.Name)
}

// A TableReplaceOperator replaces documents in the table
type TableReplaceOperator struct {
	Name string
}

// TableReplace replaces documents in the table. Incoming documents must implement the document.Keyer interface.
func TableReplace(tableName string) *TableReplaceOperator {
	return &TableReplaceOperator{Name: tableName}
}

// Op implements the Operator interface.
func (op *TableReplaceOperator) Op() (OperatorFunc, error) {
	var table *database.Table

	return func(env *expr.Environment) (*expr.Environment, error) {
		d, ok := env.GetDocument()
		if !ok {
			return nil, errors.New("missing document")
		}

		if table == nil {
			var err error
			table, err = env.GetTx().GetTable(op.Name)
			if err != nil {
				return nil, err
			}
		}

		ker, ok := d.(document.Keyer)
		if !ok {
			return nil, errors.New("missing key")
		}

		k := ker.RawKey()
		if k == nil {
			return nil, errors.New("missing key")
		}

		err := table.Replace(ker.RawKey(), d)
		if err != nil {
			return nil, err
		}

		return env, nil
	}, nil
}

func (op *TableReplaceOperator) String() string {
	return fmt.Sprintf("tableReplace('%s')", op.Name)
}

// A TableDeleteOperator replaces documents in the table
type TableDeleteOperator struct {
	Name string
}

// TableDelete deletes documents from the table. Incoming documents must implement the document.Keyer interface.
func TableDelete(tableName string) *TableDeleteOperator {
	return &TableDeleteOperator{Name: tableName}
}

// Op implements the Operator interface.
func (op *TableDeleteOperator) Op() (OperatorFunc, error) {
	var table *database.Table

	return func(env *expr.Environment) (*expr.Environment, error) {
		d, ok := env.GetDocument()
		if !ok {
			return nil, errors.New("missing document")
		}

		if table == nil {
			var err error
			table, err = env.GetTx().GetTable(op.Name)
			if err != nil {
				return nil, err
			}
		}

		ker, ok := d.(document.Keyer)
		if !ok {
			return nil, errors.New("missing key")
		}

		k := ker.RawKey()
		if k == nil {
			return nil, errors.New("missing key")
		}

		err := table.Delete(ker.RawKey())
		if err != nil {
			return nil, err
		}

		return env, nil
	}, nil
}

func (op *TableDeleteOperator) String() string {
	return fmt.Sprintf("tableDelete('%s')", op.Name)
}

// A DistinctOperator filters duplicate documents.
type DistinctOperator struct{}

// Distinct filters duplicate documents based on one or more expressions.
func Distinct() *DistinctOperator {
	return &DistinctOperator{}
}

// Op implements the Operator interface.
func (op *DistinctOperator) Op() (OperatorFunc, error) {
	var buf bytes.Buffer
	enc := document.NewValueEncoder(&buf)
	m := make(map[string]struct{})

	return func(env *expr.Environment) (*expr.Environment, error) {
		buf.Reset()

		d, ok := env.GetDocument()
		if !ok {
			return nil, errors.New("missing document")
		}

		fields, err := document.Fields(d)
		if err != nil {
			return nil, err
		}

		for _, field := range fields {
			value, err := d.GetByField(field)
			if err != nil {
				return nil, err
			}

			err = enc.Encode(value)
			if err != nil {
				return nil, err
			}
		}

		_, ok = m[string(buf.Bytes())]
		// if value already exists, filter it out
		if ok {
			return nil, nil
		}

		m[buf.String()] = struct{}{}
		return env, nil
	}, nil
}

func (op *DistinctOperator) String() string {
	return "distinct()"
}

// A SetOperator filters duplicate documents.
type SetOperator struct {
	Path document.Path
	E    expr.Expr
}

// Set filters duplicate documents based on one or more expressions.
func Set(path document.Path, e expr.Expr) *SetOperator {
	return &SetOperator{
		Path: path,
		E:    e,
	}
}

// Op implements the Operator interface.
func (op *SetOperator) Op() (OperatorFunc, error) {
	var fb document.FieldBuffer
	var newEnv expr.Environment

	return func(env *expr.Environment) (*expr.Environment, error) {
		d, ok := env.GetDocument()
		if !ok {
			return nil, errors.New("missing document")
		}

		v, err := op.E.Eval(env)
		if err != nil && err != document.ErrFieldNotFound {
			return nil, err
		}

		fb.Reset()
		err = fb.ScanDocument(d)
		if err != nil {
			return nil, err
		}

		err = fb.Set(op.Path, v)
		if err != nil {
			return nil, err
		}

		newEnv.Outer = env
		newEnv.SetDocument(&fb)

		return &newEnv, nil
	}, nil
}

func (op *SetOperator) String() string {
	return fmt.Sprintf("set(%s, %s)", op.Path, op.E)
}

// A UnsetOperator filters duplicate documents.
type UnsetOperator struct {
	Field string
}

// Unset filters duplicate documents based on one or more expressions.
func Unset(field string) *UnsetOperator {
	return &UnsetOperator{
		Field: field,
	}
}

// Op implements the Operator interface.
func (op *UnsetOperator) Op() (OperatorFunc, error) {
	var fb document.FieldBuffer
	var newEnv expr.Environment

	return func(env *expr.Environment) (*expr.Environment, error) {
		fb.Reset()

		d, ok := env.GetDocument()
		if !ok {
			return nil, errors.New("missing document")
		}

		_, err := d.GetByField(op.Field)
		if err != nil {
			if err != document.ErrFieldNotFound {
				return nil, err
			}

			return env, nil
		}

		err = fb.ScanDocument(d)
		if err != nil {
			return nil, err
		}

		err = fb.Delete(document.NewPath(op.Field))
		if err != nil {
			return nil, err
		}

		newEnv.Outer = env
		newEnv.SetDocument(&fb)

		return &newEnv, nil
	}, nil
}

func (op *UnsetOperator) String() string {
	return fmt.Sprintf("unset(%s)", op.Field)
}
