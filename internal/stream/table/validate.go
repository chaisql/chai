package table

import (
	"fmt"

	"github.com/cockroachdb/errors"
	"github.com/genjidb/genji/internal/database"
	"github.com/genjidb/genji/internal/environment"
	"github.com/genjidb/genji/internal/stream"
)

// ValidateOperator validates and converts incoming documents against table and field constraints.
type ValidateOperator struct {
	stream.BaseOperator

	tableName string
}

func Validate(tableName string) *ValidateOperator {
	return &ValidateOperator{
		tableName: tableName,
	}
}

func (op *ValidateOperator) Iterate(in *environment.Environment, fn func(out *environment.Environment) error) error {
	catalog := in.GetCatalog()
	tx := in.GetTx()

	info, err := catalog.GetTableInfo(op.tableName)
	if err != nil {
		return err
	}
	if info.ReadOnly {
		return errors.New("cannot write to read-only table")
	}

	var buf []byte

	var newEnv environment.Environment

	return op.Prev.Iterate(in, func(out *environment.Environment) error {
		buf = buf[:0]
		newEnv.SetOuter(out)

		doc, ok := out.GetDocument()
		if !ok {
			return errors.New("missing document")
		}

		// generate default values, validate and encode document
		buf, err = info.EncodeDocument(tx, buf, doc)
		if err != nil {
			return err
		}

		// use the encoded document as the new document
		doc = database.NewEncodedDocument(&info.FieldConstraints, buf)

		newEnv.SetDocument(doc)

		// validate CHECK constraints if any
		err := info.TableConstraints.ValidateDocument(tx, doc)
		if err != nil {
			return err
		}

		return fn(&newEnv)
	})
}

func (op *ValidateOperator) String() string {
	return fmt.Sprintf("table.Validate(%q)", op.tableName)
}
