package table

import (
	"fmt"

	"github.com/chaisql/chai/internal/database"
	"github.com/chaisql/chai/internal/environment"
	"github.com/chaisql/chai/internal/stream"
	"github.com/cockroachdb/errors"
)

// ValidateOperator validates and converts incoming rows against table and column constraints.
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
	tx := in.GetTx()

	info, err := tx.Catalog.GetTableInfo(op.tableName)
	if err != nil {
		return err
	}
	if info.ReadOnly {
		return errors.New("cannot write to read-only table")
	}

	var buf []byte

	var newEnv environment.Environment

	var br database.BasicRow
	var eo database.EncodedRow
	return op.Prev.Iterate(in, func(out *environment.Environment) error {
		buf = buf[:0]
		newEnv.SetOuter(out)

		row, ok := out.GetRow()
		if !ok {
			return errors.New("missing row")
		}

		// generate default values, validate and encode row
		buf, err = info.EncodeRow(tx, buf, row)
		if err != nil {
			return err
		}

		// use the encoded row as the new row
		eo.ResetWith(&info.ColumnConstraints, buf)

		if dRow, ok := row.(database.Row); ok {
			br.ResetWith(op.tableName, dRow.Key(), &eo)
			newEnv.SetRow(&br)
		} else {
			br.ResetWith(op.tableName, nil, &eo)
			newEnv.SetRow(&br)
		}

		// validate CHECK constraints if any
		err := info.TableConstraints.ValidateRow(tx, newEnv.Row)
		if err != nil {
			return err
		}

		return fn(&newEnv)
	})
}

func (op *ValidateOperator) String() string {
	return fmt.Sprintf("table.Validate(%q)", op.tableName)
}
