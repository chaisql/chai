package index

import (
	"fmt"

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

func (op *DeleteOperator) Iterate(in *environment.Environment, fn func(out *environment.Environment) error) error {
	tx := in.GetTx()

	info, err := tx.Catalog.GetIndexInfo(op.indexName)
	if err != nil {
		return err
	}

	table, err := tx.Catalog.GetTable(tx, info.Owner.TableName)
	if err != nil {
		return err
	}

	idx, err := tx.Catalog.GetIndex(tx, op.indexName)
	if err != nil {
		return err
	}

	return op.Prev.Iterate(in, func(out *environment.Environment) error {
		row, ok := out.GetDatabaseRow()
		if !ok {
			return errors.New("missing row")
		}

		old, err := table.GetRow(row.Key())
		if err != nil {
			return err
		}

		vs := make([]types.Value, 0, len(info.Columns))
		for _, column := range info.Columns {
			v, err := old.Get(column)
			if err != nil {
				v = types.NewNullValue()
			}
			vs = append(vs, v)
		}

		key, err := table.Info.EncodeKey(old.Key())
		if err != nil {
			return err
		}

		err = idx.Delete(vs, key)
		if err != nil {
			return err
		}

		return fn(out)
	})
}

func (op *DeleteOperator) String() string {
	return fmt.Sprintf("index.Delete(%q)", op.indexName)
}
