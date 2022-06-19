package table

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

// A ScanOperator iterates over the documents of a table.
type ScanOperator struct {
	stream.BaseOperator
	TableName string
	Ranges    stream.Ranges
	Reverse   bool
}

// Scan creates an iterator that iterates over each document of the given table that match the given ranges.
// If no ranges are provided, it iterates over all documents.
func Scan(tableName string, ranges ...stream.Range) *ScanOperator {
	return &ScanOperator{TableName: tableName, Ranges: ranges}
}

// ScanReverse creates an iterator that iterates over each document of the given table in reverse order.
func ScanReverse(tableName string, ranges ...stream.Range) *ScanOperator {
	return &ScanOperator{TableName: tableName, Ranges: ranges, Reverse: true}
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

// Iterate over the documents of the table. Each document is stored in the environment
// that is passed to the fn function, using SetCurrentValue.
func (it *ScanOperator) Iterate(in *environment.Environment, fn func(out *environment.Environment) error) error {
	var newEnv environment.Environment
	newEnv.SetOuter(in)
	newEnv.Set(environment.TableKey, types.NewTextValue(it.TableName))

	table, err := in.GetCatalog().GetTable(in.GetTx(), it.TableName)
	if err != nil {
		return err
	}

	var ranges []*database.Range

	if it.Ranges == nil {
		ranges = []*database.Range{nil}
	} else {
		ranges, err = it.Ranges.Eval(in)
		if err != nil {
			return err
		}
	}

	for _, rng := range ranges {
		err = table.IterateOnRange(rng, it.Reverse, func(key *tree.Key, d types.Document) error {
			newEnv.SetKey(key)
			newEnv.SetDocument(d)

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
