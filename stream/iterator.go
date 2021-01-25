package stream

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"

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

// Expressions creates an operator that iterates over the given expressions.
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
}

// SeqScan creates an iterator that iterates over each document of the given table.
func SeqScan(tableName string) *SeqScanOperator {
	return &SeqScanOperator{TableName: tableName}
}

func (it *SeqScanOperator) Iterate(in *expr.Environment, fn func(out *expr.Environment) error) error {
	table, err := in.GetTx().GetTable(it.TableName)
	if err != nil {
		return err
	}

	var newEnv expr.Environment
	newEnv.Outer = in

	return table.AscendGreaterOrEqual(document.Value{}, func(d document.Document) error {
		newEnv.SetDocument(d)
		return fn(&newEnv)
	})
}

func (it *SeqScanOperator) String() string {
	return fmt.Sprintf("seqScan(%s)", it.TableName)
}

// A PkScanOperator iterates over the documents of a table.
type PkScanOperator struct {
	baseOperator
	TableName string
	Ranges    Ranges
	Reverse   bool
}

// PkScan creates an iterator that iterates over each document of the given table.
func PkScan(tableName string, ranges ...Range) *PkScanOperator {
	return &PkScanOperator{TableName: tableName, Ranges: ranges}
}

// PkScanReverse creates an iterator that iterates over each document of the given table in reverse order.
func PkScanReverse(tableName string, ranges ...Range) *PkScanOperator {
	return &PkScanOperator{TableName: tableName, Ranges: ranges, Reverse: true}
}

func (it *PkScanOperator) String() string {
	var s strings.Builder

	s.WriteString("pkScan")
	if it.Reverse {
		s.WriteString("Reverse")
	}

	s.WriteRune('(')

	s.WriteString(strconv.Quote(it.TableName))
	if len(it.Ranges) > 0 {
		s.WriteString(", ")
		for i, r := range it.Ranges {
			s.WriteString(r.String())
			if i+1 < len(it.Ranges) {
				s.WriteString(", ")
			}
		}
	}

	s.WriteString(")")

	return s.String()
}

// Iterate over the documents of the table. Each document is stored in the environment
// that is passed to the fn function, using SetCurrentValue.
func (it *PkScanOperator) Iterate(in *expr.Environment, fn func(out *expr.Environment) error) error {
	if len(it.Ranges) == 0 {
		return SeqScan(it.TableName).Iterate(in, fn)
	}

	table, err := in.GetTx().GetTable(it.TableName)
	if err != nil {
		return err
	}

	ranges, err := it.Ranges.Encode(table, in)
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

	var iterator func(pivot document.Value, fn func(d document.Document) error) error

	if !it.Reverse {
		iterator = table.AscendGreaterOrEqual
		if lower != nil {
			start = *lower
		}
		if upper != nil {
			end = *upper
		}
	} else {
		iterator = table.DescendLessOrEqual
		if upper != nil {
			start = *upper
		}
		if lower != nil {
			end = *lower
		}
	}

	var newEnv expr.Environment
	newEnv.Outer = in

	var encEnd []byte
	if !end.Type.IsZero() {
		encEnd, err = table.EncodeValue(end)
		if err != nil {
			return err
		}
	}

	err = iterator(start, func(d document.Document) error {
		key := d.(document.Keyer).RawKey()

		// if the indexed value satisfies at least one
		// range, it gets outputted.
		if !ranges.valueIsInRange(key) {
			return nil
		}

		newEnv.SetDocument(d)
		err = fn(&newEnv)
		if err != nil {
			return err
		}

		// if we reached the end of our global range, we can stop iterating.
		if bytes.Compare(key, encEnd) < 0 {
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

	IndexName string
	Ranges    Ranges
	Reverse   bool
}

// IndexScan creates an iterator that iterates over each document of the given table.
func IndexScan(name string, ranges ...Range) *IndexScanOperator {
	return &IndexScanOperator{IndexName: name, Ranges: ranges}
}

// IndexScanReverse creates an iterator that iterates over each document of the given table in reverse order.
func IndexScanReverse(name string, ranges ...Range) *IndexScanOperator {
	return &IndexScanOperator{IndexName: name, Ranges: ranges, Reverse: true}
}

func (it *IndexScanOperator) String() string {
	var s strings.Builder

	s.WriteString("indexScan")
	if it.Reverse {
		s.WriteString("Reverse")
	}

	s.WriteRune('(')

	s.WriteString(strconv.Quote(it.IndexName))
	if len(it.Ranges) > 0 {
		s.WriteString(", ")
		for i, r := range it.Ranges {
			s.WriteString(r.String())
			if i+1 < len(it.Ranges) {
				s.WriteString(", ")
			}
		}
	}

	s.WriteString(")")

	return s.String()
}

// Iterate over the documents of the table. Each document is stored in the environment
// that is passed to the fn function, using SetCurrentValue.
func (it *IndexScanOperator) Iterate(in *expr.Environment, fn func(out *expr.Environment) error) error {
	index, err := in.GetTx().GetIndex(it.IndexName)
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

	var iterator func(pivot document.Value, fn func(val, key []byte) error) error

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
		if lower != nil {
			end = *lower
		}
	}

	var newEnv expr.Environment
	newEnv.Outer = in

	// if there are no ranges or if both the global lower bound and upper bound are nil, use a simpler and faster iteration function
	if len(it.Ranges) == 0 || (lower == nil && upper == nil) {
		return iterator(document.Value{}, func(val, key []byte) error {
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

	err = iterator(start, func(val, key []byte) error {
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
	if r.Exact {
		return fmt.Sprintf("%v", r.Min)
	}

	min, max := r.Min, r.Max
	if min == nil {
		min = expr.IntegerValue(-1)
	}
	if max == nil {
		max = expr.IntegerValue(-1)
	}

	if r.Exclusive {
		return fmt.Sprintf("[%v, %v, true]", min, max)
	}

	return fmt.Sprintf("[%v:%v]", r.Min, r.Max)
}

type Ranges []Range

type ValueEncoder interface {
	EncodeValue(v document.Value) ([]byte, error)
}

func (r Ranges) Encode(encoder ValueEncoder, env *expr.Environment) (encodedRanges, error) {
	enc := make([]encodedRange, len(r))
	for i := range r {
		if r[i].Min != nil {
			minVal, err := r[i].Min.Eval(env)
			if err != nil {
				return nil, err
			}
			enc[i].minVal = &minVal
			enc[i].min, err = encoder.EncodeValue(minVal)
			if err != nil {
				return nil, err
			}
		}
		if r[i].Max != nil {
			maxVal, err := r[i].Max.Eval(env)
			if err != nil {
				return nil, err
			}
			enc[i].maxVal = &maxVal
			enc[i].max, err = encoder.EncodeValue(maxVal)
			if err != nil {
				return nil, err
			}
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
