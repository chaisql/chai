package statement

import (
	"github.com/genjidb/genji/internal/environment"
	"github.com/genjidb/genji/internal/errors"
	"github.com/genjidb/genji/internal/planner"
	"github.com/genjidb/genji/internal/stream"
	"github.com/genjidb/genji/types"
)

// StreamStmt is a StreamStmt using a Stream.
type StreamStmt struct {
	Stream   *stream.Stream
	ReadOnly bool

	PreparedStream *stream.Stream
}

// Prepare optimizes the stream and stores it in s.
func (s *StreamStmt) Prepare(ctx *Context) error {
	var err error
	s.PreparedStream, err = planner.Optimize(s.Stream, ctx.Catalog)
	return err
}

// Run returns a result containing the stream. The stream will be executed by calling the Iterate method of
// the result.
func (s *StreamStmt) Run(ctx *Context) (Result, error) {
	if s.PreparedStream == nil {
		err := s.Prepare(ctx)
		if err != nil {
			return Result{}, err
		}
	}

	return Result{
		Iterator: &StreamStmtIterator{
			Stream:  s.PreparedStream,
			Context: ctx,
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

// StreamStmtIterator iterates over a stream.
type StreamStmtIterator struct {
	Stream  *stream.Stream
	Context *Context
}

func (s *StreamStmtIterator) Iterate(fn func(d types.Document) error) error {
	var env environment.Environment
	env.DB = s.Context.DB
	env.Tx = s.Context.Tx
	env.Catalog = s.Context.Catalog
	env.SetParams(s.Context.Params)

	err := s.Stream.Iterate(&env, func(env *environment.Environment) error {
		// if there is no doc in this specific environment,
		// the last operator is not outputting anything
		// worth returning to the user.
		if env.Doc == nil {
			return nil
		}

		return fn(env.Doc)
	})
	if errors.Is(err, stream.ErrStreamClosed) {
		err = nil
	}
	return err
}
