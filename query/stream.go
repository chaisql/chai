package query

import (
	"github.com/genjidb/genji/database"
	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/expr"
	"github.com/genjidb/genji/planner"
	"github.com/genjidb/genji/stream"
)

// StreamStmt is a StreamStmt using a Stream.
type StreamStmt struct {
	Stream   *stream.Stream
	ReadOnly bool
}

// Run returns a result containing the stream. The stream will be executed by calling the Iterate method of
// the result.
func (s *StreamStmt) Run(tx *database.Transaction, params []expr.Param) (Result, error) {
	st, err := planner.Optimize(s.Stream.Clone(), tx, params)
	if err != nil || st == nil {
		return Result{}, err
	}

	return Result{
		Iterator: &streamStmtIterator{
			Stream: st,
			Tx:     tx,
			Params: params,
		},
	}, nil
}

// IsReadOnly reports whether the stream will modify the database or only read it.
func (s *StreamStmt) IsReadOnly() bool {
	return s.ReadOnly
}

func (s *StreamStmt) String() string {
	return s.Stream.String()
}

type streamStmtIterator struct {
	Stream *stream.Stream
	Tx     *database.Transaction
	Params []expr.Param
}

func (s *streamStmtIterator) Iterate(fn func(d document.Document) error) error {
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
