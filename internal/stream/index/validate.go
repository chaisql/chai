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

	indexName string
}

func Validate(indexName string) *ValidateOperator {
	return &ValidateOperator{
		indexName: indexName,
	}
}

func (op *ValidateOperator) Iterate(in *environment.Environment, fn func(out *environment.Environment) error) error {
	tx := in.GetTx()

	info, err := tx.Catalog.GetIndexInfo(op.indexName)
	if err != nil {
		return err
	}

	if !info.Unique {
		return errors.New("indexValidate can be used only on unique indexes")
	}

	idx, err := tx.Catalog.GetIndex(tx, op.indexName)
	if err != nil {
		return err
	}

	return op.Prev.Iterate(in, func(out *environment.Environment) error {
		r, ok := out.GetRow()
		if !ok {
			return errors.New("missing row")
		}

		vs := make([]types.Value, 0, len(info.Paths))

		// if the indexes values contain NULL somewhere,
		// we don't check for unicity.
		// cf: https://sqlite.org/lang_createindex.html#unique_indexes
		var hasNull bool
		for _, path := range info.Paths {
			v, err := path.GetValueFromObject(r.Object())
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
