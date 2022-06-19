package stream

import (
	"fmt"

	"github.com/genjidb/genji/internal/database"
	"github.com/genjidb/genji/internal/environment"
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

func (op *OnConflictOperator) Iterate(in *environment.Environment, fn func(out *environment.Environment) error) error {
	var newEnv environment.Environment

	return op.Prev.Iterate(in, func(out *environment.Environment) error {
		err := fn(out)
		if err != nil {
			if cerr, ok := err.(*database.ConstraintViolationError); ok {
				if op.OnConflict == nil {
					return nil
				}

				newEnv.SetOuter(out)
				newEnv.SetKey(cerr.Key)

				err = op.OnConflict.Iterate(&newEnv, func(out *environment.Environment) error { return nil })
			}
		}
		return err
	})
}

func (op *OnConflictOperator) String() string {
	if op.OnConflict == nil {
		return "stream.OnConflict(NULL)"
	}

	return fmt.Sprintf("stream.OnConflict(%s)", op.OnConflict)
}
