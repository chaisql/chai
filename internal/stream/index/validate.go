package index

import (
	"fmt"

	"github.com/cockroachdb/errors"
	"github.com/genjidb/genji/internal/database"
	"github.com/genjidb/genji/internal/environment"
	"github.com/genjidb/genji/internal/stream"
	"github.com/genjidb/genji/types"
)

// ValidateOperator reads the input stream and deletes the document from the specified index.
type ValidateOperator struct {
	stream.BaseOperator

	indexName string
}

func Validate(indexName string) *ValidateOperator {
	return &ValidateOperator{
		indexName: indexName,
	}
}

func (op *ValidateOperator) Iterate(in *environment.Environment, fn func(out *environment.Environment) error) error {
	catalog := in.GetCatalog()
	tx := in.GetTx()

	info, err := catalog.GetIndexInfo(op.indexName)
	if err != nil {
		return err
	}

	if !info.Unique {
		return errors.New("indexValidate can be used only on unique indexes")
	}

	idx, err := catalog.GetIndex(tx, op.indexName)
	if err != nil {
		return err
	}

	return op.Prev.Iterate(in, func(out *environment.Environment) error {
		doc, ok := out.GetDocument()
		if !ok {
			return errors.New("missing document")
		}

		vs := make([]types.Value, 0, len(info.Paths))

		// if the indexes values contain NULL somewhere,
		// we don't check for unicity.
		// cf: https://sqlite.org/lang_createindex.html#unique_indexes
		var hasNull bool
		for _, path := range info.Paths {
			v, err := path.GetValueFromDocument(doc)
			if err != nil {
				hasNull = true
				v = types.NewNullValue()
			} else if v.Type() == types.NullValue {
				hasNull = true
			}

			vs = append(vs, v)
		}

		if !hasNull {
			duplicate, key, err := idx.Exists(vs)
			if err != nil {
				return err
			}
			if duplicate {
				return &database.ConstraintViolationError{
					Constraint: "UNIQUE",
					Paths:      info.Paths,
					Key:        key,
				}
			}
		}

		return fn(out)
	})
}

func (op *ValidateOperator) String() string {
	return fmt.Sprintf("index.Validate(%q)", op.indexName)
}
