package stream

import (
	"strconv"
	"strings"

	"github.com/genjidb/genji/internal/database"
	"github.com/genjidb/genji/internal/environment"
	"github.com/genjidb/genji/internal/errors"
	"github.com/genjidb/genji/internal/expr"
	"github.com/genjidb/genji/internal/stringutil"
	"github.com/genjidb/genji/internal/tree"
	"github.com/genjidb/genji/types"
)

// DocumentPointer holds a document key and lazily loads the document on demand when the Iterate or GetByField method is called.
// It implements the types.Document and the document.Keyer interfaces.
type DocumentPointer struct {
	key   []byte
	Table *database.Table
	Doc   types.Document
}

func (d *DocumentPointer) Iterate(fn func(field string, value types.Value) error) error {
	var err error
	if d.Doc == nil {
		d.Doc, err = d.Table.GetDocument(d.key)
		if err != nil {
			return err
		}
	}

	return d.Doc.Iterate(fn)
}

func (d *DocumentPointer) GetByField(field string) (types.Value, error) {
	var err error
	if d.Doc == nil {
		d.Doc, err = d.Table.GetDocument(d.key)
		if err != nil {
			return nil, err
		}
	}

	return d.Doc.GetByField(field)
}

func (d *DocumentPointer) MarshalJSON() ([]byte, error) {
	if d.Doc == nil {
		var err error
		d.Doc, err = d.Table.GetDocument(d.key)
		if err != nil {
			return nil, err
		}
	}

	return d.Doc.(interface{ MarshalJSON() ([]byte, error) }).MarshalJSON()
}

type DocumentsOperator struct {
	baseOperator
	Docs []types.Document
}

// Documents creates a DocumentsOperator that iterates over the given values.
func Documents(documents ...types.Document) *DocumentsOperator {
	return &DocumentsOperator{
		Docs: documents,
	}
}

func (op *DocumentsOperator) Iterate(in *environment.Environment, fn func(out *environment.Environment) error) error {
	var newEnv environment.Environment
	newEnv.SetOuter(in)

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

func (op *ExprsOperator) Iterate(in *environment.Environment, fn func(out *environment.Environment) error) error {
	var newEnv environment.Environment
	newEnv.SetOuter(in)

	for _, e := range op.Exprs {
		v, err := e.Eval(in)
		if err != nil {
			return err
		}
		if v.Type() != types.DocumentValue {
			return errors.Wrap(ErrInvalidResult)
		}

		newEnv.SetDocument(v.V().(types.Document))
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

func (it *SeqScanOperator) Iterate(in *environment.Environment, fn func(out *environment.Environment) error) error {
	table, err := in.GetCatalog().GetTable(in.GetTx(), it.TableName)
	if err != nil {
		return err
	}

	var newEnv environment.Environment
	newEnv.SetOuter(in)
	newEnv.Set(environment.TableKey, types.NewTextValue(it.TableName))

	return table.Iterate(nil, it.Reverse, func(key tree.Key, d types.Document) error {
		newEnv.Set(environment.DocPKKey, types.NewBlobValue(key))
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
func (it *PkScanOperator) Iterate(in *environment.Environment, fn func(out *environment.Environment) error) error {
	var newEnv environment.Environment
	newEnv.SetOuter(in)
	newEnv.Set(environment.TableKey, types.NewTextValue(it.TableName))

	table, err := in.GetCatalog().GetTable(in.GetTx(), it.TableName)
	if err != nil {
		return err
	}

	ranges, err := it.Ranges.Eval(in)
	if err != nil {
		return err
	}

	for _, rng := range ranges {
		err = table.IterateOnRange(rng, it.Reverse, func(key tree.Key, d types.Document) error {
			newEnv.Set(environment.DocPKKey, types.NewBlobValue(key))
			newEnv.SetDocument(d)

			return fn(&newEnv)
		})
		if errors.Is(err, ErrStreamClosed) {
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
	if len(ranges) == 0 {
		panic("IndexScan: no ranges specified")
	}
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
func (it *IndexScanOperator) Iterate(in *environment.Environment, fn func(out *environment.Environment) error) error {
	catalog := in.GetCatalog()
	tx := in.GetTx()

	index, err := catalog.GetIndex(tx, it.IndexName)
	if err != nil {
		return err
	}

	info, err := catalog.GetIndexInfo(it.IndexName)
	if err != nil {
		return err
	}

	table, err := catalog.GetTable(tx, info.TableName)
	if err != nil {
		return err
	}

	var newEnv environment.Environment
	newEnv.SetOuter(in)
	newEnv.Set(environment.TableKey, types.NewTextValue(table.Info.Name()))

	ranges, err := it.Ranges.Eval(in)
	if err != nil || len(ranges) != len(it.Ranges) {
		return err
	}

	ptr := DocumentPointer{
		Table: table,
	}
	newEnv.SetDocument(&ptr)

	for _, rng := range ranges {
		r, err := rng.ToTreeRange(&table.Info.FieldConstraints, info.Paths)
		if err != nil {
			return err
		}

		err = index.IterateOnRange(r, it.Reverse, func(key tree.Key) error {
			ptr.key = key
			ptr.Doc = nil
			newEnv.Set(environment.DocPKKey, types.NewBlobValue(key))

			return fn(&newEnv)
		})
		if errors.Is(err, ErrStreamClosed) {
			err = nil
		}
		if err != nil {
			return err
		}
	}

	return nil
}
