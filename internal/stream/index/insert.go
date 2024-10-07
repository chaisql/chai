package index

import (
	"fmt"

	"github.com/chaisql/chai/internal/database"
	"github.com/chaisql/chai/internal/environment"
	"github.com/chaisql/chai/internal/row"
	"github.com/chaisql/chai/internal/stream"
	"github.com/chaisql/chai/internal/types"
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
	err   error
}

func (it *InsertIterator) Next() bool {
	if !it.Iterator.Next() {
		return false
	}

	var r row.Row
	r, it.err = it.Iterator.Row()
	if it.err != nil {
		return false
	}

	vs := make([]types.Value, 0, len(it.info.Columns))
	for _, column := range it.info.Columns {
		v, err := r.Get(column)
		if err != nil {
			v = types.NewNullValue()
		}
		vs = append(vs, v)
	}

	k, err := it.Iterator.Key()
	if err != nil {
		it.err = err
		return false
	}

	encKey, err := it.tinfo.EncodeKey(k)
	if err != nil {
		it.err = err
		return false
	}

	err = it.index.Set(vs, encKey)
	if err != nil {
		it.err = fmt.Errorf("error while inserting index value: %w", err)
		return false
	}

	return true
}
