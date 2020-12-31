package stream

import (
	"bytes"
	"container/heap"
	"errors"
	"fmt"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/sql/query/expr"
)

const (
	groupEnvKey = "_group"
	accEnvKey   = "_acc"
)

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
	E expr.Expr
}

// Take closes the stream after n values have passed through the operator.
// n must evaluate to a number or to a value that can be converted to an integer.
func Take(n expr.Expr) *TakeOperator {
	return &TakeOperator{E: n}
}

// Op implements the Operator interface.
func (m *TakeOperator) Op() (OperatorFunc, error) {
	var n, count int64
	v, err := m.E.Eval(&expr.Environment{})
	if err != nil {
		return nil, err
	}
	if v.Type != document.IntegerValue {
		v, err = v.CastAsInteger()
		if err != nil {
			return nil, err
		}
	}
	n = v.V.(int64)

	return func(env *expr.Environment) (*expr.Environment, error) {
		if count < n {
			count++
			return env, nil
		}

		return nil, ErrStreamClosed
	}, nil
}

func (m *TakeOperator) String() string {
	return fmt.Sprintf("take(%s)", m.E)
}

// A SkipOperator skips the n first values of the stream.
type SkipOperator struct {
	E expr.Expr
}

// Skip ignores the first n values of the stream.
// n must evaluate to a number or to a value that can be converted to an integer.
func Skip(n expr.Expr) *SkipOperator {
	return &SkipOperator{E: n}
}

// Op implements the Operator interface.
func (m *SkipOperator) Op() (OperatorFunc, error) {
	var n, skipped int64
	v, err := m.E.Eval(&expr.Environment{})
	if err != nil {
		return nil, err
	}
	if v.Type != document.IntegerValue {
		v, err = v.CastAsInteger()
		if err != nil {
			return nil, err
		}
	}
	n = v.V.(int64)

	return func(env *expr.Environment) (*expr.Environment, error) {
		if skipped < n {
			skipped++
			return nil, nil
		}

		return env, nil
	}, nil
}

func (m *SkipOperator) String() string {
	return fmt.Sprintf("skip(%s)", m.E)
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
		newEnv.Outer = env
		return &newEnv, nil
	}, nil
}

func (op *GroupByOperator) String() string {
	return fmt.Sprintf("groupBy(%s)", op.E)
}

// A ReduceOperator consumes the given stream and outputs one value per group.
// It reads the _group variable from the environment to determine with group
// to assign each value. If no _group variable is available, it will assume all
// values are part of the same group and reduce them into one value.
// To reduce incoming values, reduce
type ReduceOperator struct {
	Seed, Accumulator expr.Expr
}

// Reduce consumes the incoming stream and outputs one value per group.
// It reads the _group variable from the environment to determine whitch group
// to assign each value. If no _group variable is available, it will assume all
// values are part of the same group and reduce them into one value.
// The seed is used to determine the initial value of the reduction. The initial value
// is stored in the _acc variable of the environment.
// The accumulator then takes the environment for each incoming value and is used to
// compute the new value of the _acc variable.
func Reduce(seed, accumulator expr.Expr) *ReduceOperator {
	return &ReduceOperator{Seed: seed, Accumulator: accumulator}
}

// Pipe stores s in the operator and return a new Stream with the reduce operator appended. It implements the Piper interface.
func (op *ReduceOperator) Pipe(s Stream) Stream {
	return Stream{
		it: IteratorFunc(func(fn func(env *expr.Environment) error) error {
			return op.iterate(s, fn)
		}),
	}
}

// Op implements the Operator interface but should never be called by Stream.
func (op *ReduceOperator) Op() (OperatorFunc, error) {
	return func(env *expr.Environment) (*expr.Environment, error) {
		return env, nil
	}, nil
}

func (op *ReduceOperator) iterate(s Stream, fn func(env *expr.Environment) error) error {
	var b bytes.Buffer
	enc := document.NewValueEncoder(&b)

	// encode null
	nullValue := document.NewNullValue()
	err := enc.Encode(nullValue)
	if err != nil {
		return err
	}
	nullKey := b.String()
	b.Reset()

	groups := make(map[string]*expr.Environment)
	var groupKeys []string

	mkGroup := func(outer *expr.Environment, groupValue document.Value, groupKey string) (*expr.Environment, error) {
		var groupEnv expr.Environment
		groupEnv.Outer = outer
		groups[groupKey] = &groupEnv
		groupKeys = append(groupKeys, groupKey)

		seed, err := op.Seed.Eval(&groupEnv)
		if err != nil {
			return nil, err
		}
		groupEnv.Set(accEnvKey, seed)

		return &groupEnv, nil
	}

	err = s.Iterate(func(env *expr.Environment) error {
		groupValue, ok := env.Get(document.NewPath(groupEnvKey))
		if !ok {
			groupValue = nullValue
		}

		b.Reset()
		err := enc.Encode(groupValue)
		if err != nil {
			return err
		}

		groupName := b.String()
		genv, ok := groups[groupName]
		if !ok {
			genv, err = mkGroup(env, groupValue, groupName)
			if err != nil {
				return err
			}
		}

		v, err := op.Accumulator.Eval(genv)
		if err != nil {
			return err
		}

		genv.Set(accEnvKey, v)
		return nil
	})
	if err != nil {
		return err
	}

	if len(groups) == 0 {
		// create one group by default if there was no input
		mkGroup(nil, nullValue, nullKey)
	}

	for _, groupKey := range groupKeys {
		genv := groups[groupKey]
		acc, _ := genv.Get(document.NewPath(accEnvKey))
		if acc.Type != document.DocumentValue {
			return ErrInvalidResult
		}

		genv.SetDocument(acc.V.(document.Document))
		err = fn(genv)
		if err != nil {
			return err
		}
	}

	return nil
}

func (op *ReduceOperator) String() string {
	return fmt.Sprintf("reduce(%s, %s)", op.Seed, op.Accumulator)
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
		it: IteratorFunc(func(fn func(env *expr.Environment) error) error {
			return op.iterate(s, fn)
		}),
	}
}

// Op implements the Operator interface but should never be called by Stream.
func (op *SortOperator) Op() (OperatorFunc, error) {
	return func(env *expr.Environment) (*expr.Environment, error) {
		return env, nil
	}, nil
}

func (op *SortOperator) iterate(s Stream, fn func(env *expr.Environment) error) error {
	h, err := op.sortStream(s)
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

func (op *SortOperator) sortStream(st Stream) (heap.Interface, error) {
	var h heap.Interface
	if op.Desc {
		h = new(maxHeap)
	} else {
		h = new(minHeap)
	}

	heap.Init(h)

	return h, st.Iterate(func(env *expr.Environment) error {
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
