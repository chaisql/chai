package index

import (
	"fmt"

	"github.com/cockroachdb/errors"
	"github.com/genjidb/genji/internal/environment"
	"github.com/genjidb/genji/internal/stream"
	"github.com/genjidb/genji/types"
)

// InsertOperator reads the input stream and indexes each object.
type InsertOperator struct {
	stream.BaseOperator

	indexName string
}

func Insert(indexName string) *InsertOperator {
	return &InsertOperator{
		indexName: indexName,
	}
}

func (op *InsertOperator) Iterate(in *environment.Environment, fn func(out *environment.Environment) error) error {
	tx := in.GetTx()

	idx, err := tx.Catalog.GetIndex(tx, op.indexName)
	if err != nil {
		return err
	}

	info, err := tx.Catalog.GetIndexInfo(op.indexName)
	if err != nil {
		return err
	}

	tinfo, err := tx.Catalog.GetTableInfo(info.Owner.TableName)
	if err != nil {
		return err
	}

	return op.Prev.Iterate(in, func(out *environment.Environment) error {
		r, ok := out.GetRow()
		if !ok {
			return errors.New("missing row")
		}

		vs := make([]types.Value, 0, len(info.Paths))
		for _, path := range info.Paths {
			v, err := path.GetValueFromObject(r.Object())
			if err != nil {
				v = types.NewNullValue()
			}
			vs = append(vs, v)
		}

		encKey, err := tinfo.EncodeKey(r.Key())
		if err != nil {
			return err
		}

		err = idx.Set(vs, encKey)
		if err != nil {
			return fmt.Errorf("error while inserting index value: %w", err)
		}

		return fn(out)
	})
}

func (op *InsertOperator) String() string {
	return fmt.Sprintf("index.Insert(%q)", op.indexName)
}
