package table

import (
	"strconv"
	"strings"

	"github.com/chaisql/chai/internal/database"
	"github.com/chaisql/chai/internal/environment"
	"github.com/chaisql/chai/internal/stream"
)

// A ScanOperator iterates over the objects of a table.
type ScanOperator struct {
	stream.BaseOperator
	TableName string
	Ranges    stream.Ranges
	Reverse   bool
	// If set, the operator will scan this table.
	// It not set, it will get the scan from the catalog.
	Table *database.Table
}

// Scan creates an iterator that iterates over each object of the given table that match the given ranges.
// If no ranges are provided, it iterates over all objects.
func Scan(tableName string, ranges ...stream.Range) *ScanOperator {
	return &ScanOperator{TableName: tableName, Ranges: ranges}
}

// ScanReverse creates an iterator that iterates over each object of the given table in reverse order.
func ScanReverse(tableName string, ranges ...stream.Range) *ScanOperator {
	return &ScanOperator{TableName: tableName, Ranges: ranges, Reverse: true}
}

func (op *ScanOperator) Iterator(in *environment.Environment) (stream.Iterator, error) {
	table := op.Table
	var err error
	if table == nil {
		table, err = in.GetTx().Catalog.GetTable(in.GetTx(), op.TableName)
		if err != nil {
			return nil, err
		}
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
		ranges:  ranges,
		reverse: op.Reverse,
	}, nil
}

func (it *ScanOperator) Columns(env *environment.Environment) ([]string, error) {
	tx := env.GetTx()

	info, err := tx.Catalog.GetTableInfo(it.TableName)
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

	s.WriteString("table.Scan")
	if it.Reverse {
		s.WriteString("Reverse")
	}

	s.WriteRune('(')

	s.WriteString(strconv.Quote(it.TableName))
	if len(it.Ranges) > 0 {
		s.WriteString(", [")
		for i, r := range it.Ranges {
			s.WriteString(r.String())
			if i+1 < len(it.Ranges) {
				s.WriteString(", ")
			}
		}
		s.WriteString("]")
	}

	s.WriteString(")")

	return s.String()
}

type ScanIterator struct {
	table   *database.Table
	ranges  []*database.Range
	reverse bool

	cursor int
	it     *database.TableIterator
	err    error
}

func (it *ScanIterator) Close() error {
	if it.it != nil {
		return it.it.Close()
	}

	return nil
}

func (it *ScanIterator) Next() bool {
	if it.it == nil {
		it.it, it.err = it.table.Iterator(it.ranges[0])
		if it.err != nil {
			return false
		}

		return it.it.Start(it.reverse)
	}

	if it.it.Move(it.reverse) {
		return true
	}

	err := it.it.Close()
	if err != nil {
		it.err = err
		return false
	}
	it.it = nil

	it.cursor++

	if it.cursor < len(it.ranges) {
		it.it, it.err = it.table.Iterator(it.ranges[it.cursor])
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

	return it.it.Value()
}
