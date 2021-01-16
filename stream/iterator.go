package stream

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/sql/query/expr"
)

type DocumentsOperator struct {
	baseOperator
	Docs []document.Document
}

// NewDocumentIterator creates an iterator that iterates over the given values.
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

// A SeqScanOperator iterates over the documents of a table.
type SeqScanOperator struct {
	baseOperator
	Name    string
	Min     document.Value
	Max     document.Value
	Reverse bool
}

// SeqScan creates an iterator that iterates over each document of the given table.
func SeqScan(name string) *SeqScanOperator {
	return &SeqScanOperator{Name: name}
}

// SeqScanOptions are used to control the iteration range and direction.
type SeqScanOptions struct {
	Min     document.Value
	Max     document.Value
	Reverse bool
}

// SeqScanWithOptions creates an iterator that iterates over each document of the given table.
func SeqScanWithOptions(name string, opt SeqScanOptions) *SeqScanOperator {
	return &SeqScanOperator{
		Name:    name,
		Min:     opt.Min,
		Max:     opt.Max,
		Reverse: opt.Reverse,
	}
}

// Iterate over the documents of the table. Each document is stored in the environment
// that is passed to the fn function, using SetCurrentValue.
func (it *SeqScanOperator) Iterate(in *expr.Environment, fn func(out *expr.Environment) error) error {
	var min, max []byte

	table, err := in.GetTx().GetTable(it.Name)
	if err != nil {
		return err
	}

	if !it.Min.Type.IsZero() {
		min, err = table.EncodeValueToKey(it.Min)
		if err != nil {
			return err
		}
	}

	if !it.Max.Type.IsZero() {
		max, err = table.EncodeValueToKey(it.Max)
		if err != nil {
			return err
		}
	}

	errStop := errors.New("stop")

	var newEnv expr.Environment
	newEnv.Outer = in

	if !it.Reverse {
		if max == nil {
			return table.AscendGreaterOrEqual(it.Min, func(d document.Document) error {
				newEnv.SetDocument(d)
				return fn(&newEnv)
			})
		}
		err := table.AscendGreaterOrEqual(it.Min, func(d document.Document) error {
			k := d.(document.Keyer).RawKey()

			// if there is an upper bound, iterate until we reach the max key
			if bytes.Compare(k, max) >= 0 {
				return errStop
			}

			newEnv.SetDocument(d)
			return fn(&newEnv)
		})
		if err == errStop {
			err = nil
		}
		return err
	}

	if min == nil {
		return table.DescendLessOrEqual(it.Max, func(d document.Document) error {
			newEnv.SetDocument(d)
			return fn(&newEnv)
		})
	}

	err = table.DescendLessOrEqual(it.Max, func(d document.Document) error {
		k := d.(document.Keyer).RawKey()

		// if there is a lower bound, iterate until we reach the min key
		if bytes.Compare(k, min) <= 0 {
			return errStop
		}

		newEnv.SetDocument(d)
		return fn(&newEnv)
	})
	if err == errStop {
		err = nil
	}
	return err
}

func (it *SeqScanOperator) String() string {
	var min, max, reverse string
	if !it.Min.Type.IsZero() {
		min = it.Min.String()
	}
	if !it.Max.Type.IsZero() {
		max = it.Max.String()
	}

	reverse = "+"
	if it.Reverse {
		reverse = "-"
	}

	return fmt.Sprintf("%s%s[%s:%s]", reverse, it.Name, min, max)
}

// A IndexScanIterator iterates over the documents of an index.
type IndexScanIterator struct {
	Name    string
	Min     expr.Expr
	Max     expr.Expr
	Reverse bool
}

// IndexScan creates an iterator that iterates over each document of the given table.
func IndexScan(name string) *IndexScanIterator {
	return &IndexScanIterator{Name: name}
}

// IndexScanOptions are used to control the iteration range and direction.
type IndexScanOptions struct {
	Min     expr.Expr
	Max     expr.Expr
	Reverse bool
}

// IndexScanWithOptions creates an iterator that iterates over each document of the given table.
func IndexScanWithOptions(name string, opt IndexScanOptions) *IndexScanIterator {
	return &IndexScanIterator{
		Name:    name,
		Min:     opt.Min,
		Max:     opt.Max,
		Reverse: opt.Reverse,
	}
}

func (it *IndexScanIterator) String() string {
	reverse := "+"
	if it.Reverse {
		reverse = "-"
	}

	return fmt.Sprintf("%s%s[%s:%s]", reverse, it.Name, it.Min, it.Max)
}

// Iterate over the documents of the table. Each document is stored in the environment
// that is passed to the fn function, using SetCurrentValue.
func (it *IndexScanIterator) Iterate(in *expr.Environment, fn func(out *expr.Environment) error) error {
	var min, max document.Value
	var bmin, bmax []byte

	index, err := in.GetTx().GetIndex(it.Name)
	if err != nil {
		return err
	}

	table, err := in.GetTx().GetTable(index.Opts.TableName)
	if err != nil {
		return err
	}

	if it.Min != nil {
		min, err = it.Min.Eval(in)
		if err != nil {
			return err
		}
		bmin, err = index.EncodeValue(min)
		if err != nil {
			return err
		}
	}

	if it.Max != nil {
		max, err := it.Max.Eval(in)
		if err != nil {
			return err
		}
		bmax, err = index.EncodeValue(max)
		if err != nil {
			return err
		}
	}

	errStop := errors.New("stop")

	var newEnv expr.Environment
	newEnv.Outer = in

	if !it.Reverse {
		if bmax == nil {
			return index.AscendGreaterOrEqual(min, func(val, key []byte, isEqual bool) error {
				d, err := table.GetDocument(key)
				if err != nil {
					return err
				}

				newEnv.SetDocument(d)
				return fn(&newEnv)
			})
		}
		err := index.AscendGreaterOrEqual(min, func(val, key []byte, isEqual bool) error {
			// if there is an upper bound, iterate until we reach the max key
			if bytes.Compare(val, bmax) >= 0 {
				return errStop
			}

			d, err := table.GetDocument(key)
			if err != nil {
				return err
			}

			newEnv.SetDocument(d)
			return fn(&newEnv)
		})
		if err == errStop {
			err = nil
		}
		return err
	}

	if bmin == nil {
		return index.DescendLessOrEqual(max, func(val, key []byte, isEqual bool) error {
			d, err := table.GetDocument(key)
			if err != nil {
				return err
			}

			newEnv.SetDocument(d)
			return fn(&newEnv)
		})
	}

	err = index.DescendLessOrEqual(max, func(val, key []byte, isEqual bool) error {
		// if there is a lower bound, iterate until we reach the min key
		if bytes.Compare(val, bmin) <= 0 {
			return errStop
		}

		d, err := table.GetDocument(key)
		if err != nil {
			return err
		}
		newEnv.SetDocument(d)
		return fn(&newEnv)
	})
	if err == errStop {
		err = nil
	}
	return err
}
