package stream

import (
	"bytes"
	"strconv"
	"strings"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/expr"
	"github.com/genjidb/genji/stringutil"
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
		sb.WriteString(d.(stringutil.Stringer).String())
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
		v, err := e.Eval(in)
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
		sb.WriteString(e.(stringutil.Stringer).String())
	}
	sb.WriteByte(')')

	return sb.String()
}

// A SeqScanOperator iterates over the documents of a table.
type SeqScanOperator struct {
	baseOperator
	TableName string
	Reverse   bool
}

// SeqScan creates an iterator that iterates over each document of the given table.
func SeqScan(tableName string) *SeqScanOperator {
	return &SeqScanOperator{TableName: tableName}
}

// SeqScanReverse creates an iterator that iterates over each document of the given table in reverse order.
func SeqScanReverse(tableName string) *SeqScanOperator {
	return &SeqScanOperator{TableName: tableName, Reverse: true}
}

func (it *SeqScanOperator) Iterate(in *expr.Environment, fn func(out *expr.Environment) error) error {
	table, err := in.GetTx().GetTable(it.TableName)
	if err != nil {
		return err
	}

	var newEnv expr.Environment
	newEnv.Outer = in

	var iterator func(pivot document.Value, fn func(d document.Document) error) error
	if !it.Reverse {
		iterator = table.AscendGreaterOrEqual
	} else {
		iterator = table.DescendLessOrEqual
	}

	return iterator(document.Value{}, func(d document.Document) error {
		newEnv.SetDocument(d)
		return fn(&newEnv)
	})
}

func (it *SeqScanOperator) String() string {
	if !it.Reverse {
		return stringutil.Sprintf("seqScan(%s)", it.TableName)
	}
	return stringutil.Sprintf("seqScanReverse(%s)", it.TableName)
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
	// if there are no ranges,  use a simpler and faster iteration function
	if len(it.Ranges) == 0 {
		s := SeqScan(it.TableName)
		s.Reverse = it.Reverse
		return s.Iterate(in, fn)
	}

	var newEnv expr.Environment
	newEnv.Outer = in

	table, err := in.GetTx().GetTable(it.TableName)
	if err != nil {
		return err
	}

	err = it.Ranges.Encode(table, in)
	if err != nil {
		return err
	}

	var iterator func(pivot document.Value, fn func(d document.Document) error) error

	if !it.Reverse {
		iterator = table.AscendGreaterOrEqual
	} else {
		iterator = table.DescendLessOrEqual
	}

	for _, rng := range it.Ranges {
		var start, end document.Value
		if !it.Reverse {
			start = rng.Min
			end = rng.Max
		} else {
			start = rng.Max
			end = rng.Min
		}

		var encEnd []byte
		if !end.Type.IsZero() && end.V != nil {
			encEnd, err = table.EncodeValue(end)
			if err != nil {
				return err
			}
		}

		err = iterator(start, func(d document.Document) error {
			key := d.(document.Keyer).RawKey()

			if !rng.IsInRange(key) {
				// if we reached the end of our range, we can stop iterating.
				if encEnd == nil {
					return nil
				}
				cmp := bytes.Compare(key, encEnd)
				if !it.Reverse && cmp > 0 {
					return ErrStreamClosed
				}
				if it.Reverse && cmp < 0 {
					return ErrStreamClosed
				}
				return nil
			}

			newEnv.SetDocument(d)
			return fn(&newEnv)
		})
		if err == ErrStreamClosed {
			err = nil
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// A IndexScanOperator iterates over the documents of an index.
type IndexScanOperator struct {
	baseOperator

	// IndexName references the index that will be used to perform the scan
	IndexName string
	// Ranges defines the boundaries of the scan, each corresponding to one value of the group of values
	// being indexed in the case of a composite index.
	Ranges Ranges
	// Reverse indicates the direction used to traverse the index.
	Reverse bool
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
		s.WriteString(it.Ranges.String())
	}

	s.WriteString(")")

	return s.String()
}

// Iterate over the documents of the table. Each document is stored in the environment
// that is passed to the fn function, using SetCurrentValue.
func (it *IndexScanOperator) Iterate(in *expr.Environment, fn func(out *expr.Environment) error) error {
	var newEnv expr.Environment
	newEnv.Outer = in

	index, err := in.GetTx().GetIndex(it.IndexName)
	if err != nil {
		return err
	}

	table, err := in.GetTx().GetTable(index.Info.TableName)
	if err != nil {
		return err
	}

	err = it.Ranges.Encode(index, in)
	if err != nil {
		return err
	}

	var iterator func(pivots []document.Value, fn func(val, key []byte) error) error

	if !it.Reverse {
		iterator = index.AscendGreaterOrEqual
	} else {
		iterator = index.DescendLessOrEqual
	}

	// if there are no ranges use a simpler and faster iteration function
	if len(it.Ranges) == 0 {
		vs := make([]document.Value, len(index.Info.Types))
		return iterator(vs, func(val, key []byte) error {
			d, err := table.GetDocument(key)
			if err != nil {
				return err
			}

			newEnv.SetDocument(d)
			return fn(&newEnv)
		})
	}

	for _, rng := range it.Ranges {
		if !index.IsComposite() {
			var start, end document.Value
			if !it.Reverse {
				start = rng.Min
				end = rng.Max
			} else {
				start = rng.Max
				end = rng.Min
			}

			var encEnd []byte
			if !end.Type.IsZero() && end.V != nil {
				encEnd, err = index.EncodeValue(end)
				if err != nil {
					return err
				}
			}

			err = iterator([]document.Value{start}, func(val, key []byte) error {
				if !rng.IsInRange(val) {
					// if we reached the end of our range, we can stop iterating.
					if encEnd == nil {
						return nil
					}
					cmp := bytes.Compare(val, encEnd)
					if !it.Reverse && cmp > 0 {
						return ErrStreamClosed
					}
					if it.Reverse && cmp < 0 {
						return ErrStreamClosed
					}
					return nil
				}

				d, err := table.GetDocument(key)
				if err != nil {
					return err
				}

				newEnv.SetDocument(d)
				return fn(&newEnv)
			})
			if err == ErrStreamClosed {
				err = nil
			}
			if err != nil {
				return err
			}
		} else {
			var start, end document.Value
			if !it.Reverse {
				start = rng.Min
				end = rng.Max
			} else {
				start = rng.Max
				end = rng.Min
			}

			var encEnd []byte
			if end.V != nil {
				encEnd, err = index.EncodeValue(end)
				if err != nil {
					return err
				}
			}

			// extract the pivots from the range, which in the case of a composite index is an array
			pivots := []document.Value{}
			if start.V != nil {
				start.V.(document.Array).Iterate(func(i int, value document.Value) error {
					pivots = append(pivots, value)
					return nil
				})
			} else {
				for i := 0; i < index.Arity(); i++ {
					pivots = append(pivots, document.Value{})
				}
			}

			err = iterator(pivots, func(val, key []byte) error {
				if !rng.IsInRange(val) {
					// if we reached the end of our range, we can stop iterating.
					if encEnd == nil {
						return nil
					}
					cmp := bytes.Compare(val, encEnd)
					if !it.Reverse && cmp > 0 {
						return ErrStreamClosed
					}
					if it.Reverse && cmp < 0 {
						return ErrStreamClosed
					}
					return nil
				}

				d, err := table.GetDocument(key)
				if err != nil {
					return err
				}

				newEnv.SetDocument(d)
				return fn(&newEnv)
			})

			if err == ErrStreamClosed {
				err = nil
			}
			if err != nil {
				return err
			}

		}
	}

	return nil
}

type Range struct {
	Min, Max document.Value
	// Exclude Min and Max from the results.
	// By default, min and max are inclusive.
	// Exclusive and Exact cannot be set to true at the same time.
	Exclusive bool
	// Used to match an exact value equal to Min.
	// If set to true, Max will be ignored for comparison
	// and for determining the global upper bound.
	Exact bool

	// Arity represents the range arity in the case of comparing the range
	// to a composite index. With IndexArityMax, it enables to deal with the
	// cases of a composite range specifying boundaries partially, ie:
	// - Index on (a, b, c)
	// - Range is defining a max only for a and b
	// Then Arity is set to 2 and IndexArityMax is set to 3
	//
	// On
	// This field is subject to change when the support for composite index is added
	// to the query planner in an ulterior pull-request.
	Arity int

	// IndexArityMax represents the underlying Index arity.
	//
	// This field is subject to change when the support for composite index is added
	// to the query planner in an ulterior pull-request.
	IndexArityMax          int
	encodedMin, encodedMax []byte
	rangeType              document.ValueType
}

func (r *Range) encode(encoder ValueEncoder, env *expr.Environment) error {
	var err error

	// first we evaluate Min and Max
	if !r.Min.Type.IsZero() {
		r.encodedMin, err = encoder.EncodeValue(r.Min)
		if err != nil {
			return err
		}
		r.rangeType = r.Min.Type
	}
	if !r.Max.Type.IsZero() {
		r.encodedMax, err = encoder.EncodeValue(r.Max)
		if err != nil {
			return err
		}
		if !r.rangeType.IsZero() && r.rangeType != r.Max.Type {
			panic("range contain values of different types")
		}

		r.rangeType = r.Max.Type
	}

	// ensure boundaries are typed
	if r.Min.Type.IsZero() {
		r.Min.Type = r.rangeType
	}
	if r.Max.Type.IsZero() {
		r.Max.Type = r.rangeType
	}

	if r.Exclusive && r.Exact {
		panic("exclusive and exact cannot both be true")
	}

	return nil
}

func (r *Range) String() string {
	if r.Exact {
		return stringutil.Sprintf("%v", r.Min)
	}

	if r.Min.Type.IsZero() {
		r.Min = document.NewIntegerValue(-1)
	}
	if r.Max.Type.IsZero() {
		r.Max = document.NewIntegerValue(-1)
	}

	if r.Exclusive {
		return stringutil.Sprintf("[%v, %v, true]", r.Min, r.Max)
	}

	return stringutil.Sprintf("[%v, %v]", r.Min, r.Max)
}

func (r *Range) IsEqual(other *Range) bool {
	if r.Exact != other.Exact {
		return false
	}

	if r.rangeType != other.rangeType {
		return false
	}

	if r.Exclusive != other.Exclusive {
		return false
	}

	if r.Min.Type != other.Min.Type {
		return false
	}
	ok, err := r.Min.IsEqual(other.Min)
	if err != nil || !ok {
		return false
	}

	if r.Max.Type != other.Max.Type {
		return false
	}
	ok, err = r.Max.IsEqual(other.Max)
	if err != nil || !ok {
		return false
	}

	return true
}

type Ranges []Range

// Append rng to r and return the new slice.
// Duplicate ranges are ignored.
func (r Ranges) Append(rng Range) Ranges {
	// ensure we don't keep duplicate ranges
	isDuplicate := false
	for _, e := range r {
		if e.IsEqual(&rng) {
			isDuplicate = true
			break
		}
	}

	if isDuplicate {
		return r
	}

	return append(r, rng)
}

type ValueEncoder interface {
	EncodeValue(v document.Value) ([]byte, error)
}

// Encode each range using the given value encoder.
func (r Ranges) Encode(encoder ValueEncoder, env *expr.Environment) error {
	for i := range r {
		err := r[i].encode(encoder, env)
		if err != nil {
			return err
		}
	}

	return nil
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

// Cost is a best effort function to determine the cost of
// a range lookup.
func (r Ranges) Cost() int {
	var cost int

	for _, rng := range r {
		// if we are looking for an exact value
		// increment by 1
		if rng.Exact {
			cost++
			continue
		}

		// if there are two boundaries, increment by 50
		if !rng.Min.Type.IsZero() && !rng.Max.Type.IsZero() {
			cost += 50
		}

		// if there is only one boundary, increment by 100
		if (!rng.Min.Type.IsZero() && rng.Max.Type.IsZero()) || (rng.Min.Type.IsZero() && !rng.Max.Type.IsZero()) {
			cost += 100
			continue
		}

		// if there are no boundaries, increment by 200
		cost += 200
	}

	return cost
}

func (r *Range) IsInRange(value []byte) bool {
	// by default, we consider the value within range
	cmpMin, cmpMax := 1, -1

	// we compare with the lower bound and see if it matches
	if r.encodedMin != nil {
		cmpMin = bytes.Compare(value, r.encodedMin)
	}

	// if exact is true the value has to be equal to the lower bound.
	if r.Exact {
		return cmpMin == 0
	}

	// if exclusive and the value is equal to the lower bound
	// we can ignore it
	if r.Exclusive && cmpMin == 0 {
		return false
	}

	// the value is bigger than the lower bound,
	// see if it matches the upper bound.
	if r.encodedMax != nil {
		if r.IndexArityMax < r.Arity {
			cmpMax = bytes.Compare(value[:len(r.encodedMax)-1], r.encodedMax)
		} else {
			cmpMax = bytes.Compare(value, r.encodedMax)
		}
	}

	// if boundaries are strict, ignore values equal to the max
	if r.Exclusive && cmpMax == 0 {
		return false
	}

	return cmpMin >= 0 && cmpMax <= 0
}
