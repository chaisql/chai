package index

import (
	"fmt"

	"github.com/chaisql/chai/internal/database"
	"github.com/chaisql/chai/internal/environment"
	"github.com/chaisql/chai/internal/stream"
	"github.com/chaisql/chai/internal/types"
	"github.com/cockroachdb/errors"
)

// DeleteOperator reads the input stream and deletes the object from the specified index.
type DeleteOperator struct {
	stream.BaseOperator

	indexName string
}

func Delete(indexName string) *DeleteOperator {
	return &DeleteOperator{
		indexName: indexName,
	}
}

func (op *DeleteOperator) Clone() stream.Operator {
	return &DeleteOperator{
		BaseOperator: op.BaseOperator.Clone(),
		indexName:    op.indexName,
	}
}

func (op *DeleteOperator) Iterator(in *environment.Environment) (stream.Iterator, error) {
	tx := in.GetTx()

	info, err := tx.Catalog.GetIndexInfo(op.indexName)
	if err != nil {
		return nil, err
	}

	table, err := tx.Catalog.GetTable(tx, info.Owner.TableName)
	if err != nil {
		return nil, err
	}

	idx, err := tx.Catalog.GetIndex(tx, op.indexName)
	if err != nil {
		return nil, err
	}

	prev, err := op.Prev.Iterator(in)
	if err != nil {
		return nil, err
	}

	return &DeleteIterator{
		Iterator: prev,
		table:    table,
		info:     info,
		index:    idx,
	}, nil
}

func (op *DeleteOperator) String() string {
	return fmt.Sprintf("index.Delete(%q)", op.indexName)
}

type DeleteIterator struct {
	stream.Iterator

	table *database.Table
	info  *database.IndexInfo
	index *database.Index
	row   database.Row
	err   error
}

func (it *DeleteIterator) Next() bool {
	if !it.Iterator.Next() {
		return false
	}

	it.row, it.err = it.Iterator.Row()
	if it.err != nil {
		return false
	}
	if it.row == nil || it.row.Key() == nil {
		it.err = errors.New("missing row")
		return false
	}

	old, err := it.table.GetRow(it.row.Key())
	if err != nil {
		it.err = err
		return false
	}

	vs := make([]types.Value, 0, len(it.info.Columns))
	for _, column := range it.info.Columns {
		v, err := old.Get(column)
		if err != nil {
			v = types.NewNullValue()
		}
		vs = append(vs, v)
	}

	encKey, err := it.table.Info.EncodeKey(old.Key())
	if err != nil {
		it.err = err
		return false
	}

	err = it.index.Delete(vs, encKey)
	if err != nil {
		it.err = fmt.Errorf("error while deleting index value: %w", err)
		return false
	}

	return true
}

func (it *DeleteIterator) Row() (database.Row, error) {
	return it.row, it.err
}

func (it *DeleteIterator) Error() error {
	if it.err != nil {
		return it.err
	}

	return it.Iterator.Error()
}
