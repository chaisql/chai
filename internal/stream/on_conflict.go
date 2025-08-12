package stream

import (
	"fmt"

	"github.com/chaisql/chai/internal/database"
	"github.com/chaisql/chai/internal/environment"
	"github.com/chaisql/chai/internal/row"
	"github.com/cockroachdb/errors"
)

// OnConflictOperator handles any conflicts that occur during the iteration.
type OnConflictOperator struct {
	BaseOperator

	OnConflict *Stream
}

func OnConflict(onConflict *Stream) *OnConflictOperator {
	return &OnConflictOperator{
		OnConflict: onConflict,
	}
}

func (it *OnConflictOperator) Clone() Operator {
	return &OnConflictOperator{
		BaseOperator: it.BaseOperator.Clone(),
		OnConflict:   it.OnConflict.Clone(),
	}
}

func (op *OnConflictOperator) Iterator(in *environment.Environment) (Iterator, error) {
	prev, err := op.Prev.Iterator(in)
	if err != nil {
		return nil, err
	}

	return &OnConflictIterator{
		Iterator:   prev,
		OnConflict: op.OnConflict,
	}, nil
}

func (op *OnConflictOperator) String() string {
	if op.OnConflict == nil {
		return "stream.OnConflict(NULL)"
	}

	return fmt.Sprintf("stream.OnConflict(%s)", op.OnConflict)
}

type OnConflictIterator struct {
	Iterator

	OnConflict *Stream
}

func (it *OnConflictIterator) Row() (row.Row, error) {
	r, err := it.Iterator.Row()
	if err == nil {
		return r, nil
	}

	cerr, ok := err.(*database.ConstraintViolationError)
	if !ok {
		return nil, err
	}

	if it.OnConflict == nil {
		return nil, nil
	}

	dr, ok := it.Iterator.Env().GetDatabaseRow()
	if !ok {
		return nil, errors.New("missing row")
	}

	var newEnv environment.Environment
	newEnv.SetOuter(it.Iterator.Env())

	var br database.BasicRow
	br.ResetWith(dr.TableName(), cerr.Key, r)

	newIt, err := it.OnConflict.Op.Iterator(&newEnv)
	if err != nil {
		return nil, err
	}

	return newIt.Row()
}
