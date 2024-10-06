package index

import (
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"

	"github.com/chaisql/chai/internal/database"
	"github.com/chaisql/chai/internal/environment"
	"github.com/chaisql/chai/internal/stream"
	"github.com/chaisql/chai/internal/tree"
)

// A ScanOperator iterates over the objects of an index.
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

// Scan creates an iterator that iterates over each object of the given table.
func Scan(name string, ranges ...stream.Range) *ScanOperator {
	return &ScanOperator{IndexName: name, Ranges: ranges}
}

// ScanReverse creates an iterator that iterates over each object of the given table in reverse order.
func ScanReverse(name string, ranges ...stream.Range) *ScanOperator {
	return &ScanOperator{IndexName: name, Ranges: ranges, Reverse: true}
}

func (op *ScanOperator) Clone() stream.Operator {
	return &ScanOperator{
		BaseOperator: op.BaseOperator.Clone(),
		IndexName:    op.IndexName,
		Ranges:       op.Ranges.Clone(),
		Reverse:      op.Reverse,
	}
}

// Iterate over the objects of the table. Each object is stored in the environment
// that is passed to the fn function, using SetCurrentValue.
func (it *ScanOperator) Iterate(in *environment.Environment, fn func(out *environment.Environment) error) error {
	tx := in.GetTx()

	index, err := tx.Catalog.GetIndex(tx, it.IndexName)
	if err != nil {
		return err
	}

	info, err := tx.Catalog.GetIndexInfo(it.IndexName)
	if err != nil {
		return err
	}

	table, err := tx.Catalog.GetTable(tx, info.Owner.TableName)
	if err != nil {
		return err
	}

	var newEnv environment.Environment
	newEnv.SetOuter(in)

	var ptr database.LazyRow

	newEnv.SetRow(&ptr)

	if len(it.Ranges) == 0 {
		return it.iterateOverRange(table, index, info, nil, &newEnv, &ptr, fn)
	}

	ranges, err := it.Ranges.Eval(in)
	if err != nil || len(ranges) != len(it.Ranges) {
		return err
	}

	for _, rng := range ranges {
		err = it.iterateOverRange(table, index, info, rng, &newEnv, &ptr, fn)
		if err != nil {
			return err
		}
	}

	return nil
}

func (op *ScanOperator) iterateOverRange(table *database.Table, index *database.Index, info *database.IndexInfo, rng *database.Range, to *environment.Environment, ptr *database.LazyRow, fn func(out *environment.Environment) error) error {
	var r *tree.Range
	var err error

	if rng != nil {
		r, err = rng.ToTreeRange(&table.Info.ColumnConstraints, info.Columns)
		if err != nil {
			return err
		}
	}

	it, err := index.Iterator(r)
	if err != nil {
		return err
	}
	defer it.Close()

	if !op.Reverse {
		it.First()
	} else {
		it.Last()
	}

	for it.Valid() {
		key, err := it.Value()
		if err != nil {
			return err
		}

		ptr.ResetWith(table, key)

		err = fn(to)
		if errors.Is(err, stream.ErrStreamClosed) {
			break
		}
		if err != nil {
			return err
		}

		if !op.Reverse {
			it.Next()
		} else {
			it.Prev()
		}
	}

	return it.Error()
}

func (it *ScanOperator) Columns(env *environment.Environment) ([]string, error) {
	tx := env.GetTx()

	idxInfo, err := tx.Catalog.GetIndexInfo(it.IndexName)
	if err != nil {
		return nil, err
	}

	info, err := tx.Catalog.GetTableInfo(idxInfo.Owner.TableName)
	if err != nil {
		return nil, err
	}

	columns := make([]string, len(info.ColumnConstraints.Ordered))
	for i, c := range info.ColumnConstraints.Ordered {
		columns[i] = c.Column
	}

	return columns, nil
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
