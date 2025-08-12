package table

import (
	"fmt"

	"github.com/chaisql/chai/internal/database"
	"github.com/chaisql/chai/internal/environment"
	"github.com/chaisql/chai/internal/row"
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

func (op *ValidateOperator) Clone() stream.Operator {
	return &ValidateOperator{
		BaseOperator: op.BaseOperator.Clone(),
		tableName:    op.tableName,
	}
}

func (op *ValidateOperator) Iterator(in *environment.Environment) (stream.Iterator, error) {
	tx := in.GetTx()

	info, err := tx.Catalog.GetTableInfo(op.tableName)
	if err != nil {
		return nil, err
	}
	if info.ReadOnly {
		return nil, errors.New("cannot write to read-only table")
	}

	prev, err := op.Prev.Iterator(in)
	if err != nil {
		return nil, err
	}

	return &ValidateIterator{
		Iterator:  prev,
		tableName: op.tableName,
		tx:        tx,
		info:      info,
	}, nil
}

func (op *ValidateOperator) String() string {
	return fmt.Sprintf("table.Validate(%q)", op.tableName)
}

type ValidateIterator struct {
	stream.Iterator

	tableName string
	tx        *database.Transaction
	info      *database.TableInfo
	buf       []byte
	br        database.BasicRow
	eo        database.EncodedRow
}

func (it *ValidateIterator) Row() (row.Row, error) {
	it.buf = it.buf[:0]

	r, err := it.Iterator.Row()
	if err != nil {
		return nil, err
	}

	// generate default values, validate and encode row
	it.buf, err = it.info.EncodeRow(it.tx, it.buf, r)
	if err != nil {
		return nil, err
	}

	// use the encoded row as the new row
	it.eo.ResetWith(&it.info.ColumnConstraints, it.buf)

	if dRow, ok := r.(database.Row); ok {
		it.br.ResetWith(it.tableName, dRow.Key(), &it.eo)
	} else {
		it.br.ResetWith(it.tableName, nil, &it.eo)
	}

	// validate CHECK constraints if any
	err = it.info.TableConstraints.ValidateRow(it.tx, &it.br)
	if err != nil {
		return nil, err
	}

	return &it.br, nil
}
