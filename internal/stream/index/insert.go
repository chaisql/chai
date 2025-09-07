package index

import (
	"fmt"

	"github.com/chaisql/chai/internal/database"
	"github.com/chaisql/chai/internal/environment"
	"github.com/chaisql/chai/internal/stream"
	"github.com/chaisql/chai/internal/types"
	"github.com/cockroachdb/errors"
)

// InsertOperator reads the input stream and indexes each row.
type InsertOperator struct {
	stream.BaseOperator

	indexName string
}

func Insert(indexName string) *InsertOperator {
	return &InsertOperator{
		indexName: indexName,
	}
}

func (op *InsertOperator) Clone() stream.Operator {
	return &InsertOperator{
		BaseOperator: op.BaseOperator.Clone(),
		indexName:    op.indexName,
	}
}

func (op *InsertOperator) Iterator(in *environment.Environment) (stream.Iterator, error) {
	tx := in.GetTx()

	idx, err := tx.Catalog.GetIndex(tx, op.indexName)
	if err != nil {
		return nil, err
	}

	info, err := tx.Catalog.GetIndexInfo(op.indexName)
	if err != nil {
		return nil, err
	}

	tinfo, err := tx.Catalog.GetTableInfo(info.Owner.TableName)
	if err != nil {
		return nil, err
	}

	prev, err := op.Prev.Iterator(in)
	if err != nil {
		return nil, err
	}

	return &InsertIterator{
		Iterator: prev,
		index:    idx,
		tinfo:    tinfo,
		info:     info,
	}, nil
}

func (op *InsertOperator) String() string {
	return fmt.Sprintf("index.Insert(%q)", op.indexName)
}

type InsertIterator struct {
	stream.Iterator

	tinfo *database.TableInfo
	info  *database.IndexInfo
	index *database.Index
	row   database.Row
	err   error
}

func (it *InsertIterator) Next() bool {
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

	vs := make([]types.Value, 0, len(it.info.Columns))
	for _, column := range it.info.Columns {
		v, err := it.row.Get(column)
		if err != nil {
			v = types.NewNullValue()
		}
		vs = append(vs, v)
	}

	k := it.row.Key()

	encKey, err := it.tinfo.EncodeKey(k)
	if err != nil {
		it.err = err
		return false
	}

	it.err = it.index.Set(vs, encKey)
	return it.err == nil
}

func (it *InsertIterator) Error() error {
	if it.err != nil {
		return it.err
	}

	return it.Iterator.Error()
}

func (it *InsertIterator) Row() (database.Row, error) {
	return it.row, it.Error()
}
