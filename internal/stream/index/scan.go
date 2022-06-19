package index

import (
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"

	"github.com/genjidb/genji/internal/database"
	"github.com/genjidb/genji/internal/environment"
	"github.com/genjidb/genji/internal/stream"
	"github.com/genjidb/genji/internal/tree"
	"github.com/genjidb/genji/types"
)

// A ScanOperator iterates over the documents of an index.
type ScanOperator struct {
	stream.BaseOperator

	// IndexName references the index that will be used to perform the scan
	IndexName string
	// Ranges defines the boundaries of the scan, each corresponding to one value of the group of values
	// being indexed in the case of a composite index.
	Ranges stream.Ranges
	// Reverse indicates the direction used to traverse the index.
	Reverse bool
}

// Scan creates an iterator that iterates over each document of the given table.
func Scan(name string, ranges ...stream.Range) *ScanOperator {
	return &ScanOperator{IndexName: name, Ranges: ranges}
}

// ScanReverse creates an iterator that iterates over each document of the given table in reverse order.
func ScanReverse(name string, ranges ...stream.Range) *ScanOperator {
	return &ScanOperator{IndexName: name, Ranges: ranges, Reverse: true}
}

// Iterate over the documents of the table. Each document is stored in the environment
// that is passed to the fn function, using SetCurrentValue.
func (it *ScanOperator) Iterate(in *environment.Environment, fn func(out *environment.Environment) error) error {
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

	table, err := catalog.GetTable(tx, info.Owner.TableName)
	if err != nil {
		return err
	}

	var newEnv environment.Environment
	newEnv.SetOuter(in)
	newEnv.Set(environment.TableKey, types.NewTextValue(table.Info.TableName))

	ptr := DocumentPointer{
		Table: table,
	}
	newEnv.SetDocument(&ptr)

	if len(it.Ranges) == 0 {
		return index.IterateOnRange(nil, it.Reverse, func(key *tree.Key) error {
			ptr.key = key
			ptr.Doc = nil
			newEnv.SetKey(key)

			return fn(&newEnv)
		})
	}

	ranges, err := it.Ranges.Eval(in)
	if err != nil || len(ranges) != len(it.Ranges) {
		return err
	}

	for _, rng := range ranges {
		r, err := rng.ToTreeRange(&table.Info.FieldConstraints, info.Paths)
		if err != nil {
			return err
		}

		err = index.IterateOnRange(r, it.Reverse, func(key *tree.Key) error {
			ptr.key = key
			ptr.Doc = nil
			newEnv.SetKey(key)

			return fn(&newEnv)
		})
		if errors.Is(err, stream.ErrStreamClosed) {
			err = nil
		}
		if err != nil {
			return err
		}
	}

	return nil
}

func (it *ScanOperator) String() string {
	var s strings.Builder

	s.WriteString("index.Scan")
	if it.Reverse {
		s.WriteString("Reverse")
	}

	s.WriteRune('(')

	s.WriteString(strconv.Quote(it.IndexName))
	if len(it.Ranges) > 0 {
		s.WriteString(", [")
		s.WriteString(it.Ranges.String())
		s.WriteString("]")
	}

	s.WriteString(")")

	return s.String()
}

// DocumentPointer holds a document key and lazily loads the document on demand when the Iterate or GetByField method is called.
// It implements the types.Document and the document.Keyer interfaces.
type DocumentPointer struct {
	key   *tree.Key
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
