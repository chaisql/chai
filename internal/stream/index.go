package stream

import (
	"strconv"
	"strings"

	errs "github.com/genjidb/genji/errors"
	"github.com/genjidb/genji/internal/environment"
	"github.com/genjidb/genji/internal/errors"
	"github.com/genjidb/genji/internal/stringutil"
	"github.com/genjidb/genji/internal/tree"
	"github.com/genjidb/genji/types"
)

// IndexValidateOperator reads the input stream and deletes the document from the specified index.
type IndexValidateOperator struct {
	baseOperator

	indexName string
}

func IndexValidate(indexName string) *IndexValidateOperator {
	return &IndexValidateOperator{
		indexName: indexName,
	}
}

func (op *IndexValidateOperator) Iterate(in *environment.Environment, fn func(out *environment.Environment) error) error {
	catalog := in.GetCatalog()
	tx := in.GetTx()

	info, err := catalog.GetIndexInfo(op.indexName)
	if err != nil {
		return err
	}

	if !info.Unique {
		return errors.New("indexValidate can be used only on unique indexes")
	}

	idx, err := catalog.GetIndex(tx, op.indexName)
	if err != nil {
		return err
	}

	var newEnv environment.Environment

	return op.Prev.Iterate(in, func(out *environment.Environment) error {
		newEnv.SetOuter(out)

		doc, ok := out.GetDocument()
		if !ok {
			return errors.New("missing document")
		}

		vs := make([]types.Value, 0, len(info.Paths))

		for _, path := range info.Paths {
			v, err := path.GetValueFromDocument(doc)
			if err != nil {
				v = types.NewNullValue()
			}

			vs = append(vs, v)
		}

		duplicate, key, err := idx.Exists(vs)
		if err != nil {
			return err
		}
		if duplicate {
			return &errs.ConstraintViolationError{
				Constraint: "UNIQUE",
				Paths:      info.Paths,
				Key:        key,
			}
		}

		return fn(&newEnv)
	})
}

func (op *IndexValidateOperator) String() string {
	return stringutil.Sprintf("indexValidate(%q)", op.indexName)
}

// IndexInsertOperator reads the input stream and indexes each document.
type IndexInsertOperator struct {
	baseOperator

	indexName string
}

func IndexInsert(indexName string) *IndexInsertOperator {
	return &IndexInsertOperator{
		indexName: indexName,
	}
}

func (op *IndexInsertOperator) Iterate(in *environment.Environment, fn func(out *environment.Environment) error) error {
	catalog := in.GetCatalog()
	tx := in.GetTx()

	idx, err := catalog.GetIndex(tx, op.indexName)
	if err != nil {
		return err
	}

	info, err := catalog.GetIndexInfo(op.indexName)
	if err != nil {
		return err
	}

	return op.Prev.Iterate(in, func(out *environment.Environment) error {
		d, ok := out.GetDocument()
		if !ok {
			return errors.New("missing document")
		}

		key, ok := out.Get(environment.DocPKKey)
		if !ok {
			return errors.New("missing document key")
		}

		vs := make([]types.Value, 0, len(info.Paths))
		for _, path := range info.Paths {
			v, err := path.GetValueFromDocument(d)
			if err != nil {
				v = types.NewNullValue()
			}
			vs = append(vs, v)
		}

		err = idx.Set(vs, key.V().([]byte))
		if err != nil {
			return stringutil.Errorf("error while inserting index value: %w", err)
		}

		return fn(out)
	})
}

func (op *IndexInsertOperator) String() string {
	return stringutil.Sprintf("indexInsert(%q)", op.indexName)
}

// IndexDeleteOperator reads the input stream and deletes the document from the specified index.
type IndexDeleteOperator struct {
	baseOperator

	indexName string
}

func IndexDelete(indexName string) *IndexDeleteOperator {
	return &IndexDeleteOperator{
		indexName: indexName,
	}
}

func (op *IndexDeleteOperator) Iterate(in *environment.Environment, fn func(out *environment.Environment) error) error {
	catalog := in.GetCatalog()
	tx := in.GetTx()

	info, err := catalog.GetIndexInfo(op.indexName)
	if err != nil {
		return err
	}

	table, err := catalog.GetTable(tx, info.TableName)
	if err != nil {
		return err
	}

	idx, err := catalog.GetIndex(tx, op.indexName)
	if err != nil {
		return err
	}

	return op.Prev.Iterate(in, func(out *environment.Environment) error {
		dk, ok := out.Get(environment.DocPKKey)
		if !ok {
			return errors.New("missing document key")
		}

		key := tree.Key(dk.V().([]byte))

		old, err := table.GetDocument(key)
		if err != nil {
			return err
		}

		info, err := catalog.GetIndexInfo(op.indexName)
		if err != nil {
			return err
		}

		vs := make([]types.Value, 0, len(info.Paths))
		for _, path := range info.Paths {
			v, err := path.GetValueFromDocument(old)
			if err != nil {
				v = types.NewNullValue()
			}
			vs = append(vs, v)
		}

		err = idx.Delete(vs, key)
		if err != nil {
			return err
		}

		return fn(out)
	})
}

func (op *IndexDeleteOperator) String() string {
	return stringutil.Sprintf("indexDelete(%q)", op.indexName)
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
