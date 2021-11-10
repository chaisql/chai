package stream

import (
	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/internal/environment"
	"github.com/genjidb/genji/internal/errors"
	"github.com/genjidb/genji/internal/stringutil"
	"github.com/genjidb/genji/types"
)

// IndexInsertOperator reads the input stream and indexes each document.
type IndexInsertOperator struct {
	baseOperator

	indexName string
}

func IndexInsert(indexName string) *IndexInsertOperator {
	return &IndexInsertOperator{
		indexName: indexName,
	}
}

func (op *IndexInsertOperator) Iterate(in *environment.Environment, fn func(out *environment.Environment) error) error {
	catalog := in.GetCatalog()
	tx := in.GetTx()

	idx, err := catalog.GetIndex(tx, op.indexName)
	if err != nil {
		return err
	}

	return op.Prev.Iterate(in, func(out *environment.Environment) error {
		d, ok := out.GetDocument()
		if !ok {
			return errors.New("missing document")
		}

		values := make([]types.Value, len(idx.Info.Paths))
		for i, path := range idx.Info.Paths {
			values[i], err = path.GetValueFromDocument(d)
			if errors.Is(err, document.ErrFieldNotFound) {
				return nil
			}
			if err != nil {
				return err
			}

		}
		err = idx.Set(values, d.(document.Keyer).RawKey())
		if err != nil {
			return stringutil.Errorf("error while building the index: %w", err)
		}

		return fn(out)
	})
}

func (op *IndexInsertOperator) String() string {
	return stringutil.Sprintf("indexInsert(%q)", op.indexName)
}
