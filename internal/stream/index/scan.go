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
		return index.IterateOnRange(nil, it.Reverse, func(key *tree.Key) error {
			ptr.ResetWith(table, key)

			return fn(&newEnv)
		})
	}

	ranges, err := it.Ranges.Eval(in)
	if err != nil || len(ranges) != len(it.Ranges) {
		return err
	}

	for _, rng := range ranges {
		r, err := rng.ToTreeRange(&table.Info.ColumnConstraints, info.Columns)
		if err != nil {
			return err
		}

		err = index.IterateOnRange(r, it.Reverse, func(key *tree.Key) error {
			ptr.ResetWith(table, key)

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
