package planner

import (
	"github.com/genjidb/genji/database"
	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/query"
	"github.com/genjidb/genji/query/expr"
	"github.com/genjidb/genji/stream"
)

// Statement is a query.Statement using a Stream.
type Statement struct {
	Stream   *stream.Stream
	ReadOnly bool
}

// Run returns a result containing the stream. The stream will be executed by calling the Iterate method of
// the result.
func (s *Statement) Run(tx *database.Transaction, params []expr.Param) (query.Result, error) {
	st, err := Optimize(s.Stream, tx)
	if err != nil || st == nil {
		return query.Result{}, err
	}

	return query.Result{
		Iterator: &statementIterator{
			Stream: st,
			Tx:     tx,
			Params: params,
		},
	}, nil
}

// IsReadOnly reports whether the stream will modify the database or only read it.
func (s *Statement) IsReadOnly() bool {
	return s.ReadOnly
}

func (s *Statement) String() string {
	return s.Stream.String()
}

type statementIterator struct {
	Stream *stream.Stream
	Tx     *database.Transaction
	Params []expr.Param
}

func (s *statementIterator) Iterate(fn func(d document.Document) error) error {
	env := expr.Environment{
		Tx:     s.Tx,
		Params: s.Params,
	}

	err := s.Stream.Iterate(&env, func(env *expr.Environment) error {
		// if there is no doc in this specific environment,
		// the last operator is not outputting anything
		// worth returning to the user.
		if env.Doc == nil {
			return nil
		}

		return fn(env.Doc)
	})
	if err == stream.ErrStreamClosed {
		err = nil
	}
	return err
}
