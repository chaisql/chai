package index

import (
	"fmt"

	"github.com/chaisql/chai/internal/database"
	"github.com/chaisql/chai/internal/environment"
	"github.com/chaisql/chai/internal/stream"
	"github.com/chaisql/chai/internal/types"
	"github.com/cockroachdb/errors"
)

// ValidateOperator reads the input stream and deletes the object from the specified index.
type ValidateOperator struct {
	stream.BaseOperator

	IndexName           string
	OnConflict          *stream.Stream
	OnConflictDoNothing bool
}

func Validate(indexName string) *ValidateOperator {
	return &ValidateOperator{
		IndexName: indexName,
	}
}

func ValidateOnConflict(indexName string, onConflict *stream.Stream) *ValidateOperator {
	return &ValidateOperator{
		IndexName:  indexName,
		OnConflict: onConflict,
	}
}

func ValidateOnConflictDoNothing(indexName string) *ValidateOperator {
	return &ValidateOperator{
		IndexName:           indexName,
		OnConflictDoNothing: true,
	}
}

func (op *ValidateOperator) Iterator(in *environment.Environment) (stream.Iterator, error) {
	tx := in.GetTx()

	info, err := tx.Catalog.GetIndexInfo(op.IndexName)
	if err != nil {
		return nil, err
	}

	if !info.Unique {
		return nil, errors.New("indexValidate can be used only on unique indexes")
	}

	cols, err := op.Columns(in)
	if err != nil {
		return nil, err
	}

	idx, err := tx.Catalog.GetIndex(tx, op.IndexName)
	if err != nil {
		return nil, err
	}

	prev, err := op.Prev.Iterator(in)
	if err != nil {
		return nil, err
	}

	return &ValidateIterator{
		Iterator:            prev,
		env:                 in,
		info:                info,
		columns:             cols,
		index:               idx,
		onConflict:          op.OnConflict,
		onConflictDoNothing: op.OnConflictDoNothing,
	}, nil
}

func (op *ValidateOperator) String() string {
	return fmt.Sprintf("index.Validate(%q)", op.IndexName)
}

type ValidateIterator struct {
	stream.Iterator

	env                 *environment.Environment
	info                *database.IndexInfo
	index               *database.Index
	columns             []string
	onConflict          *stream.Stream
	onConflictDoNothing bool
	row                 database.Row
	err                 error
	br                  database.BasicRow
}

func (it *ValidateIterator) Next() bool {
	for it.Iterator.Next() {
		it.row, it.err = it.Iterator.Row()
		if it.err != nil {
			return false
		}

		vs := make([]types.Value, 0, len(it.info.Columns))

		// if the indexes values contain NULL somewhere,
		// we don't check for unicity.
		// cf: https://sqlite.org/lang_createindex.html#unique_indexes
		var hasNull bool
		for _, column := range it.info.Columns {
			v, err := it.row.Get(column)
			if err != nil {
				hasNull = true
				v = types.NewNullValue()
			} else if v.Type() == types.TypeNull {
				hasNull = true
			}

			vs = append(vs, v)
		}

		if hasNull {
			return true
		}

		duplicate, key, err := it.index.Exists(vs)
		if err != nil {
			it.err = err
			return false
		}
		if !duplicate {
			return true
		}

		if it.onConflict == nil && !it.onConflictDoNothing {
			it.err = &database.ConstraintViolationError{
				Constraint: "UNIQUE",
				Columns:    it.info.Columns,
				Key:        key,
			}
			return false
		}

		// skip if ON CONFLICT DO NOTHING
		if it.onConflictDoNothing {
			continue
		}

		// use the key of original row
		it.br.ResetWith(it.row.TableName(), key, it.row)

		// execute the onConflict stream
		stream.InsertBefore(it.onConflict.Op, stream.Rows(it.columns, &it.br))
		newIt, err := it.onConflict.Iterator(it.env)
		if err != nil {
			it.err = err
			return false
		}

		for newIt.Next() {
		}
		if err := newIt.Error(); err != nil {
			_ = newIt.Close()
			it.err = err
			return false
		}

		err = newIt.Close()
		if err != nil {
			it.err = err
			return false
		}
	}

	return false
}

func (it *ValidateIterator) Row() (database.Row, error) {
	return it.row, it.err
}

func (it *ValidateIterator) Error() error {
	return it.err
}
