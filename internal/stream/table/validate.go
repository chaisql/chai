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

	TableName string
}

func Validate(tableName string) *ValidateOperator {
	return &ValidateOperator{
		TableName: tableName,
	}
}

func (op *ValidateOperator) Clone() stream.Operator {
	return &ValidateOperator{
		BaseOperator: op.BaseOperator.Clone(),
		TableName:    op.TableName,
	}
}

func (op *ValidateOperator) Iterator(in *environment.Environment) (stream.Iterator, error) {
	tx := in.GetTx()

	table, err := tx.Catalog.GetTable(tx, op.TableName)
	if err != nil {
		return nil, err
	}
	if table.Info.ReadOnly {
		return nil, errors.New("cannot write to read-only table")
	}

	cols, err := op.Columns(in)
	if err != nil {
		return nil, err
	}

	prev, err := op.Prev.Iterator(in)
	if err != nil {
		return nil, err
	}

	return &ValidateIterator{
		Iterator:  prev,
		tableName: op.TableName,
		env:       in,
		tx:        tx,
		table:     table,
		columns:   cols,
	}, nil
}

func (op *ValidateOperator) String() string {
	return fmt.Sprintf("table.Validate(%q)", op.TableName)
}

type ValidateIterator struct {
	stream.Iterator

	tableName string
	env       *environment.Environment
	tx        *database.Transaction
	table     *database.Table
	columns   []string

	buf []byte
	br  database.BasicRow
	eo  database.EncodedRow
	err error
}

func (it *ValidateIterator) Next() bool {
	if !it.Iterator.Next() {
		return false
	}

	it.buf = it.buf[:0]

	r, err := it.Iterator.Row()
	if err != nil {
		it.err = err
		return false
	}

	// generate default values, validate and encode row
	it.buf, err = it.table.Info.EncodeRow(it.tx, it.buf, r)
	if err != nil {
		it.err = err
		return false
	}

	// use the encoded row as the new row
	it.eo.ResetWith(&it.table.Info.ColumnConstraints, it.buf)
	it.br.ResetWith(it.tableName, r.Key(), &it.eo)

	// validate CHECK constraints if any
	err = it.table.Info.TableConstraints.ValidateRow(it.tx, &it.br)
	if err != nil {
		it.err = err
		return false
	}

	return true
}

func (it *ValidateIterator) Row() (database.Row, error) {
	return &it.br, it.err
}

func (it *ValidateIterator) Error() error {
	if it.err != nil {
		return it.err
	}

	return it.Iterator.Error()
}
