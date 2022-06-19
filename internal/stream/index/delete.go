package index

import (
	"fmt"

	"github.com/cockroachdb/errors"
	"github.com/genjidb/genji/internal/environment"
	"github.com/genjidb/genji/internal/stream"
	"github.com/genjidb/genji/types"
)

// DeleteOperator reads the input stream and deletes the document from the specified index.
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
	catalog := in.GetCatalog()
	tx := in.GetTx()

	info, err := catalog.GetIndexInfo(op.indexName)
	if err != nil {
		return err
	}

	table, err := catalog.GetTable(tx, info.Owner.TableName)
	if err != nil {
		return err
	}

	idx, err := catalog.GetIndex(tx, op.indexName)
	if err != nil {
		return err
	}

	return op.Prev.Iterate(in, func(out *environment.Environment) error {
		key, ok := out.GetKey()
		if !ok {
			return errors.New("missing document key")
		}

		old, err := table.GetDocument(key)
		if err != nil {
			return err
		}

		info, err := catalog.GetIndexInfo(op.indexName)
		if err != nil {
			return err
		}

		vs := make([]types.Value, 0, len(info.Paths))
		for _, path := range info.Paths {
			v, err := path.GetValueFromDocument(old)
			if err != nil {
				v = types.NewNullValue()
			}
			vs = append(vs, v)
		}

		err = idx.Delete(vs, key.Encoded)
		if err != nil {
			return err
		}

		return fn(out)
	})
}

func (op *DeleteOperator) String() string {
	return fmt.Sprintf("index.Delete(%q)", op.indexName)
}
