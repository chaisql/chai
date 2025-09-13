package index

import (
	"strconv"
	"strings"

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

func (op *ScanOperator) Iterator(in *environment.Environment) (stream.Iterator, error) {
	tx := in.GetTx()

	index, err := tx.Catalog.GetIndex(tx, op.IndexName)
	if err != nil {
		return nil, err
	}

	info, err := tx.Catalog.GetIndexInfo(op.IndexName)
	if err != nil {
		return nil, err
	}

	table, err := tx.Catalog.GetTable(tx, info.Owner.TableName)
	if err != nil {
		return nil, err
	}

	var ranges []*database.Range

	if len(op.Ranges) == 0 {
		ranges = []*database.Range{nil}
	} else {
		ranges, err = op.Ranges.Eval(in)
		if err != nil {
			return nil, err
		}
	}

	return &ScanIterator{
		table:   table,
		index:   index,
		info:    info,
		ranges:  ranges,
		reverse: op.Reverse,
	}, nil
}

type ScanIterator struct {
	table   *database.Table
	index   *database.Index
	info    *database.IndexInfo
	ranges  []*database.Range
	reverse bool

	cursor int
	it     *database.IndexIterator
	err    error
	lr     database.LazyRow
}

func (it *ScanIterator) Close() error {
	if it.it != nil {
		return it.it.Close()
	}

	return nil
}

func (it *ScanIterator) Next() bool {
	var r *tree.Range

	if it.it == nil {
		rng := it.ranges[0]
		if rng != nil {
			r, it.err = rng.ToTreeRange(&it.table.Info.ColumnConstraints, it.info.Columns)
			if it.err != nil {
				return false
			}
		}

		it.it, it.err = it.index.Iterator(r)
		if it.err != nil {
			return false
		}

		return it.it.Start(it.reverse)
	}

	if it.it.Move(it.reverse) {
		return true
	}

	it.it.Close()
	it.it = nil

	it.cursor++

	if it.cursor < len(it.ranges) {
		rng := it.ranges[it.cursor]
		if rng != nil {
			r, it.err = rng.ToTreeRange(&it.table.Info.ColumnConstraints, it.info.Columns)
			if it.err != nil {
				return false
			}
		}

		it.it, it.err = it.index.Iterator(r)
		if it.err != nil {
			return false
		}

		return it.it.Start(it.reverse)
	}

	return false
}

func (it *ScanIterator) Error() error {
	return it.err
}

func (it *ScanIterator) Row() (database.Row, error) {
	if it.err != nil {
		return nil, it.err
	}

	if it.it == nil {
		return nil, nil
	}

	key, err := it.it.Value()
	if err != nil {
		return nil, err
	}

	it.lr.ResetWith(it.table, key)

	return &it.lr, nil
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
