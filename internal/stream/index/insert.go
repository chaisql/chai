package index

import (
	"fmt"

	"github.com/cockroachdb/errors"
	"github.com/genjidb/genji/internal/environment"
	"github.com/genjidb/genji/internal/stream"
	"github.com/genjidb/genji/types"
)

// InsertOperator reads the input stream and indexes each document.
type InsertOperator struct {
	stream.BaseOperator

	indexName string
}

func IndexInsert(indexName string) *InsertOperator {
	return &InsertOperator{
		indexName: indexName,
	}
}

func (op *InsertOperator) Iterate(in *environment.Environment, fn func(out *environment.Environment) error) error {
	catalog := in.GetCatalog()
	tx := in.GetTx()

	idx, err := catalog.GetIndex(tx, op.indexName)
	if err != nil {
		return err
	}

	info, err := catalog.GetIndexInfo(op.indexName)
	if err != nil {
		return err
	}

	return op.Prev.Iterate(in, func(out *environment.Environment) error {
		d, ok := out.GetDocument()
		if !ok {
			return errors.New("missing document")
		}

		key, ok := out.GetKey()
		if !ok {
			return errors.New("missing document key")
		}

		vs := make([]types.Value, 0, len(info.Paths))
		for _, path := range info.Paths {
			v, err := path.GetValueFromDocument(d)
			if err != nil {
				v = types.NewNullValue()
			}
			vs = append(vs, v)
		}

		err = idx.Set(vs, key.Encoded)
		if err != nil {
			return fmt.Errorf("error while inserting index value: %w", err)
		}

		return fn(out)
	})
}

func (op *InsertOperator) String() string {
	return fmt.Sprintf("index.Insert(%q)", op.indexName)
}
