package table

import (
	"fmt"

	"github.com/chaisql/chai/internal/database"
	"github.com/chaisql/chai/internal/environment"
	"github.com/chaisql/chai/internal/stream"
	"github.com/chaisql/chai/internal/tree"
	"github.com/cockroachdb/errors"
)

// GenerateKeyOperator
type GenerateKeyOperator struct {
	stream.BaseOperator

	TableName           string
	OnConflict          *stream.Stream
	OnConflictDoNothing bool
}

func GenerateKey(tableName string) *GenerateKeyOperator {
	return &GenerateKeyOperator{
		TableName: tableName,
	}
}

func GenerateKeyOnConflict(tableName string, onConflict *stream.Stream) *GenerateKeyOperator {
	return &GenerateKeyOperator{
		TableName:  tableName,
		OnConflict: onConflict,
	}
}

func GenerateKeyOnConflictDoNothing(tableName string) *GenerateKeyOperator {
	return &GenerateKeyOperator{
		TableName:           tableName,
		OnConflictDoNothing: true,
	}
}

func (op *GenerateKeyOperator) Iterator(in *environment.Environment) (stream.Iterator, error) {
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

	return &GenerateKeyIterator{
		Iterator:            prev,
		tableName:           op.TableName,
		env:                 in,
		tx:                  tx,
		table:               table,
		columns:             cols,
		onConflict:          op.OnConflict,
		onConflictDoNothing: op.OnConflictDoNothing,
	}, nil
}

func (op *GenerateKeyOperator) String() string {
	if op.OnConflictDoNothing {
		return fmt.Sprintf("table.GenerateKeyOnConflictDoNothing(%q)", op.TableName)
	}
	if op.OnConflict != nil {
		return fmt.Sprintf("table.GenerateKeyOnConflict(%q, %v)", op.TableName, op.OnConflict)
	}

	return fmt.Sprintf("table.GenerateKey(%q)", op.TableName)
}

type GenerateKeyIterator struct {
	stream.Iterator

	tableName           string
	env                 *environment.Environment
	tx                  *database.Transaction
	table               *database.Table
	columns             []string
	onConflict          *stream.Stream
	onConflictDoNothing bool

	buf []byte
	br  database.BasicRow
	err error
}

func (it *GenerateKeyIterator) Next() bool {
	for it.Iterator.Next() {
		it.buf = it.buf[:0]

		r, err := it.Iterator.Row()
		if err != nil {
			it.err = err
			return false
		}

		k, err := it.generateKey(r)
		if err != nil {
			it.err = err
			return false
		}
		if k == nil {
			// if the key is nil, it means the row was not inserted due to a conflict,
			// we must skip the row
			continue
		}

		// reset the buffered row with the new key
		it.br.ResetWith(it.tableName, k, r)

		return true
	}

	return false
}

func (it *GenerateKeyIterator) Row() (database.Row, error) {
	return &it.br, it.Error()
}

func (it *GenerateKeyIterator) Error() error {
	if it.err != nil {
		return it.err
	}

	return it.Iterator.Error()
}

func (it *GenerateKeyIterator) generateKey(r database.Row) (*tree.Key, error) {
	var isRowid bool
	k, isRowid, err := it.table.GenerateKey(r)
	if err != nil {
		return nil, err
	}

	if isRowid {
		return k, nil
	}

	// check if the primary key already exists
	exists, err := it.table.Exists(k)
	if err != nil {
		return nil, err
	}
	if !exists {
		return k, nil
	}

	if it.onConflict == nil && !it.onConflictDoNothing {
		return nil, &database.ConstraintViolationError{
			Constraint: "PRIMARY KEY",
			Columns:    it.table.Info.PrimaryKey.Columns,
			Key:        k,
		}
	}

	if it.onConflictDoNothing {
		// return nil key to signal there was a conflict
		return nil, nil
	}

	it.br.ResetWith(it.tableName, k, r)

	// execute the onConflict stream
	stream.InsertBefore(it.onConflict.Op, stream.Rows(it.columns, &it.br))

	newIt, err := it.onConflict.Iterator(it.env)
	if err != nil {
		return nil, err
	}

	for newIt.Next() {
	}
	if err := newIt.Error(); err != nil {
		_ = newIt.Close()
		return nil, err
	}

	err = newIt.Close()
	if err != nil {
		return nil, err
	}

	// return nil key to signal there was a conflict
	return nil, nil
}
