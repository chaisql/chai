package tree

import (
	"container/heap"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/document/encoding"
	"github.com/genjidb/genji/index"
	"github.com/genjidb/genji/pkg/bytesutil"
	"github.com/genjidb/genji/sql/query/expr"
	"github.com/genjidb/genji/sql/scanner"
)

type sortNode struct {
	node

	sortField expr.FieldSelector
	direction scanner.Token
	st        document.Stream
}

// NewSortNode creates a node that sorts a stream according to a given
// document field and a sort direction.
func NewSortNode(n Node, sortField expr.FieldSelector, direction scanner.Token) Node {
	if direction == 0 {
		direction = scanner.ASC
	}

	return &sortNode{
		node: node{
			op:   Sort,
			left: n,
		},
		sortField: sortField,
		direction: direction,
	}
}

func (n *sortNode) toStream(st document.Stream, stack expr.EvalStack) (document.Stream, expr.EvalStack, error) {
	n.st = st
	return document.NewStream(n), stack, nil
}

// toStream operates a partial sort on the iterator using a heap.
// This ensures a O(k+n log n) time complexity, where k is the sum of
// OFFSET + LIMIT clauses, if provided, otherwise k = n.
// If the sorting is in ascending order, a min-heap will be used
// otherwise a max-heap will be used instead.
// Once the heap is filled entirely with the content of the table a stream is returned.
// During iteration, the stream will pop the k-smallest or k-largest elements, depending on
// the chosen sorting order (ASC or DESC).
// This function is not memory efficient as it's loading the entire table in memory before
// returning the k-smallest or k-largest elements.
func (n *sortNode) sortStream(st document.Stream) (heap.Interface, error) {
	path := document.ValuePath(n.sortField)

	var h heap.Interface
	if n.direction == scanner.ASC {
		h = new(minHeap)
	} else {
		h = new(maxHeap)
	}

	heap.Init(h)

	return h, st.Iterate(func(d document.Document) error {
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
}

func (n *sortNode) Iterate(fn func(d document.Document) error) error {
	h, err := n.sortStream(n.st)
	if err != nil {
		return err
	}

	for h.Len() > 0 {
		err := fn(encoding.EncodedDocument(heap.Pop(h).(heapNode).data))
		if err != nil {
			return err
		}
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
