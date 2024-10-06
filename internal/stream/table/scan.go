package table

import (
	"strconv"
	"strings"

	"github.com/chaisql/chai/internal/database"
	"github.com/chaisql/chai/internal/environment"
	"github.com/chaisql/chai/internal/stream"
	"github.com/cockroachdb/errors"
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

func (op *ScanOperator) Clone() stream.Operator {
	return &ScanOperator{
		BaseOperator: op.BaseOperator.Clone(),
		TableName:    op.TableName,
		Ranges:       op.Ranges.Clone(),
		Reverse:      op.Reverse,
		Table:        op.Table,
	}
}

// Iterate over the objects of the table. Each object is stored in the environment
// that is passed to the fn function, using SetCurrentValue.
func (op *ScanOperator) Iterate(in *environment.Environment, fn func(out *environment.Environment) error) error {
	var newEnv environment.Environment
	newEnv.SetOuter(in)

	table := op.Table
	var err error
	if table == nil {
		table, err = in.GetTx().Catalog.GetTable(in.GetTx(), op.TableName)
		if err != nil {
			return err
		}
	}

	var ranges []*database.Range

	if op.Ranges == nil {
		ranges = []*database.Range{nil}
	} else {
		ranges, err = op.Ranges.Eval(in)
		if err != nil {
			return err
		}
	}

	for _, rng := range ranges {
		err = op.iterateOverRange(table, rng, &newEnv, fn)
		if err != nil {
			return err
		}
	}

	return nil
}

func (op *ScanOperator) iterateOverRange(table *database.Table, rng *database.Range, to *environment.Environment, fn func(out *environment.Environment) error) error {
	it, err := table.Iterator(rng)
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
		row, err := it.Value()
		if err != nil {
			return err
		}
		to.SetRow(row)
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
