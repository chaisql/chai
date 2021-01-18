package stream

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/genjidb/genji/database"
	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/sql/query/expr"
)

type DocumentsOperator struct {
	baseOperator
	Docs []document.Document
}

// Documents creates a DocumentsOperator that iterates over the given values.
func Documents(documents ...document.Document) *DocumentsOperator {
	return &DocumentsOperator{
		Docs: documents,
	}
}

func (op *DocumentsOperator) Iterate(in *expr.Environment, fn func(out *expr.Environment) error) error {
	var newEnv expr.Environment
	newEnv.Outer = in

	for _, d := range op.Docs {
		newEnv.SetDocument(d)
		err := fn(&newEnv)
		if err != nil {
			return err
		}
	}

	return nil
}

func (op *DocumentsOperator) String() string {
	var sb strings.Builder

	sb.WriteString("docs(")
	for i, d := range op.Docs {
		if i > 0 {
			sb.WriteString(", ")
		}
		fmt.Fprintf(&sb, "%s", d)
	}
	sb.WriteString(")")

	return sb.String()
}

type ExprsOperator struct {
	baseOperator
	Exprs []expr.Expr
}

// NewExprIterator creates an iterator that iterates over the given expressions.
// Each expression must evaluate to a document.
func Expressions(exprs ...expr.Expr) *ExprsOperator {
	return &ExprsOperator{Exprs: exprs}
}

func (op *ExprsOperator) Iterate(in *expr.Environment, fn func(out *expr.Environment) error) error {
	var newEnv expr.Environment
	newEnv.Outer = in

	for _, e := range op.Exprs {
		v, err := e.Eval(&newEnv)
		if err != nil {
			return err
		}
		if v.Type != document.DocumentValue {
			return ErrInvalidResult
		}

		newEnv.SetDocument(v.V.(document.Document))
		err = fn(&newEnv)
		if err != nil {
			return err
		}
	}

	return nil
}

func (op *ExprsOperator) String() string {
	var sb strings.Builder

	sb.WriteString("exprs(")
	for i, e := range op.Exprs {
		if i > 0 {
			sb.WriteString(", ")
		}
		fmt.Fprintf(&sb, "%v", e)
	}

	return sb.String()
}

// A SeqScanOperator iterates over the documents of a table.
type SeqScanOperator struct {
	baseOperator
	TableName string
	Ranges    Ranges
	Reverse   bool
}

// SeqScan creates an iterator that iterates over each document of the given table.
func SeqScan(tableName string, ranges ...Range) *SeqScanOperator {
	return &SeqScanOperator{TableName: tableName, Ranges: ranges}
}

func (it *SeqScanOperator) String() string {
	reverse := "+"
	if it.Reverse {
		reverse = "-"
	}

	return fmt.Sprintf("%s%s(%s)", reverse, it.TableName, it.Ranges)
}

// Iterate over the documents of the table. Each document is stored in the environment
// that is passed to the fn function, using SetCurrentValue.
func (it *SeqScanOperator) Iterate(in *expr.Environment, fn func(out *expr.Environment) error) error {
	table, err := in.GetTx().GetTable(it.TableName)
	if err != nil {
		return err
	}

	ranges, err := it.Ranges.Encode(index, in)
	if err != nil {
		return err
	}
	// To avoid reading the entire index, we determine what is the global range we want to read.
	var start, end document.Value

	lower, err := ranges.lowerBound()
	if err != nil {
		return err
	}
	upper, err := ranges.upperBound()
	if err != nil {
		return err
	}

	var iterator func(pivot document.Value, fn func(val, key []byte, isEqual bool) error) error

	if !it.Reverse {
		iterator = index.AscendGreaterOrEqual
		if lower != nil {
			start = *lower
		}
		if upper != nil {
			end = *upper
		}
	} else {
		iterator = index.DescendLessOrEqual
		if upper != nil {
			start = *upper
		}
		if upper != nil {
			end = *lower
		}
	}

	var newEnv expr.Environment
	newEnv.Outer = in

	// if there are no ranges or if both the global lower bound and upper bound are nil, use a simpler and faster iteration function
	if len(it.Ranges) == 0 || (lower == nil && upper == nil) {
		return iterator(document.Value{}, func(val, key []byte, isEqual bool) error {
			d, err := table.GetDocument(key)
			if err != nil {
				return err
			}

			newEnv.SetDocument(d)
			return fn(&newEnv)
		})
	}

	var encEnd []byte
	if !end.Type.IsZero() {
		encEnd, err = index.EncodeValue(end)
		if err != nil {
			return err
		}
	}

	err = iterator(start, func(val, key []byte, isEqual bool) error {
		// if the indexed value satisfies at least one
		// range, it gets outputted.
		if !ranges.valueIsInRange(val) {
			return nil
		}

		d, err := table.GetDocument(key)
		if err != nil {
			return err
		}

		newEnv.SetDocument(d)
		err = fn(&newEnv)
		if err != nil {
			return err
		}

		// if we reached the end of our global range, we can stop iterating.
		if bytes.Compare(val, encEnd) < 0 {
			return ErrStreamClosed
		}

		return nil
	})
	if err == ErrStreamClosed {
		err = nil
	}
	return err
}

// A IndexScanOperator iterates over the documents of an index.
type IndexScanOperator struct {
	baseOperator

	Name    string
	Ranges  Ranges
	Reverse bool
}

// IndexScan creates an iterator that iterates over each document of the given table.
func IndexScan(name string, ranges ...Range) *IndexScanOperator {
	return &IndexScanOperator{Name: name, Ranges: ranges}
}

func (it *IndexScanOperator) String() string {
	reverse := "+"
	if it.Reverse {
		reverse = "-"
	}

	return fmt.Sprintf("%s%s(%s)", reverse, it.Name, it.Ranges)
}

// Iterate over the documents of the table. Each document is stored in the environment
// that is passed to the fn function, using SetCurrentValue.
func (it *IndexScanOperator) Iterate(in *expr.Environment, fn func(out *expr.Environment) error) error {
	index, err := in.GetTx().GetIndex(it.Name)
	if err != nil {
		return err
	}

	table, err := in.GetTx().GetTable(index.Opts.TableName)
	if err != nil {
		return err
	}

	ranges, err := it.Ranges.Encode(index, in)
	if err != nil {
		return err
	}
	// To avoid reading the entire index, we determine what is the global range we want to read.
	var start, end document.Value

	lower, err := ranges.lowerBound()
	if err != nil {
		return err
	}
	upper, err := ranges.upperBound()
	if err != nil {
		return err
	}

	var iterator func(pivot document.Value, fn func(val, key []byte, isEqual bool) error) error

	if !it.Reverse {
		iterator = index.AscendGreaterOrEqual
		if lower != nil {
			start = *lower
		}
		if upper != nil {
			end = *upper
		}
	} else {
		iterator = index.DescendLessOrEqual
		if upper != nil {
			start = *upper
		}
		if upper != nil {
			end = *lower
		}
	}

	var newEnv expr.Environment
	newEnv.Outer = in

	// if there are no ranges or if both the global lower bound and upper bound are nil, use a simpler and faster iteration function
	if len(it.Ranges) == 0 || (lower == nil && upper == nil) {
		return iterator(document.Value{}, func(val, key []byte, isEqual bool) error {
			d, err := table.GetDocument(key)
			if err != nil {
				return err
			}

			newEnv.SetDocument(d)
			return fn(&newEnv)
		})
	}

	var encEnd []byte
	if !end.Type.IsZero() {
		encEnd, err = index.EncodeValue(end)
		if err != nil {
			return err
		}
	}

	err = iterator(start, func(val, key []byte, isEqual bool) error {
		// if the indexed value satisfies at least one
		// range, it gets outputted.
		if !ranges.valueIsInRange(val) {
			return nil
		}

		d, err := table.GetDocument(key)
		if err != nil {
			return err
		}

		newEnv.SetDocument(d)
		err = fn(&newEnv)
		if err != nil {
			return err
		}

		// if we reached the end of our global range, we can stop iterating.
		if bytes.Compare(val, encEnd) < 0 {
			return ErrStreamClosed
		}

		return nil
	})
	if err == ErrStreamClosed {
		err = nil
	}
	return err
}

type Range struct {
	Min, Max expr.Expr
	// Exclude Min and Max from the results.
	// By default, min and max are inclusive.
	// Exclusive and Exact cannot be set to true at the same time.
	Exclusive bool
	// Used to match an exact value equal to Min.
	// If set to true, Max will be ignored for comparison
	// and for determining the global upper bound.
	Exact bool
}

func (r *Range) String() string {
	if r.Exclusive {
		return fmt.Sprintf("]%v:%v[", r.Min, r.Max)
	}
	if r.Exact {
		return fmt.Sprintf("%v", r.Min)
	}
	return fmt.Sprintf("[%v:%v]", r.Min, r.Max)
}

type Ranges []Range

func (r Ranges) Encode(index *database.Index, env *expr.Environment) (encodedRanges, error) {
	enc := make([]encodedRange, len(r))
	for i := range r {
		minVal, err := r[i].Min.Eval(env)
		if err != nil {
			return nil, err
		}
		enc[i].minVal = &minVal
		enc[i].min, err = index.EncodeValue(minVal)
		if err != nil {
			return nil, err
		}
		maxVal, err := r[i].Max.Eval(env)
		if err != nil {
			return nil, err
		}
		enc[i].maxVal = &maxVal
		enc[i].max, err = index.EncodeValue(maxVal)
		if err != nil {
			return nil, err
		}
		enc[i].exclusive = r[i].Exclusive
		enc[i].exact = r[i].Exact
		if enc[i].exclusive && enc[i].exact {
			panic("exclusive and exact cannot both be true")
		}
	}

	return enc, nil
}

func (r Ranges) String() string {
	var sb strings.Builder

	for i, rr := range r {
		if i > 0 {
			sb.WriteString(", ")
		}

		sb.WriteString(rr.String())
	}

	return sb.String()
}

type encodedRange struct {
	minVal, maxVal *document.Value
	min, max       []byte
	exclusive      bool
	exact          bool
}

func (e *encodedRange) IsInRange(value []byte) bool {
	// by default, we consider the value within range
	cmpMin, cmpMax := 1, -1

	// we compare with the lower bound and see if it matches
	if e.min != nil {
		cmpMin = bytes.Compare(value, e.min)
	}

	// if exact is true the value has to be equal to the lower bound.
	if e.exact {
		return cmpMin == 0
	}

	// if exclusive and the value is equal to the lower bound
	// we can ignore it
	if e.exclusive && cmpMin == 0 {
		return false
	}

	// the value is bigger than the lower bound,
	// see if it matches the upper bound.
	if e.max != nil {
		cmpMax = bytes.Compare(value, e.max)
	}

	// if boundaries are strict, ignore values equal to the max
	if e.exclusive && cmpMax == 0 {
		return false
	}

	return true
}

type encodedRanges []encodedRange

func (e encodedRanges) valueIsInRange(value []byte) bool {
	for _, r := range e {
		if r.IsInRange(value) {
			return true
		}
	}

	return false
}

// lowerBound returns the minimum value to read.
func (e encodedRanges) lowerBound() (*document.Value, error) {
	var m *document.Value
	for i := range e {
		if e[i].minVal == nil {
			return nil, nil
		}

		if m == nil {
			m = e[i].minVal
			continue
		}

		ok, err := m.IsLesserThan(*e[i].minVal)
		if err != nil {
			return nil, err
		}
		if ok {
			m = e[i].minVal
		}
	}

	return m, nil
}

// upperBoundRanges returns the maximum value to read.
func (e encodedRanges) upperBound() (*document.Value, error) {
	var m *document.Value
	for i := range e {
		if e[i].maxVal == nil {
			return nil, nil
		}

		if m == nil {
			m = e[i].maxVal
			continue
		}

		ok, err := m.IsLesserThan(*e[i].maxVal)
		if err != nil {
			return nil, err
		}
		if ok {
			m = e[i].maxVal
		}
	}

	return m, nil
}
