package statement

import (
	"github.com/cockroachdb/errors"
	"github.com/genjidb/genji/internal/environment"
	"github.com/genjidb/genji/internal/planner"
	"github.com/genjidb/genji/internal/stream"
	"github.com/genjidb/genji/types"
)

// StreamStmt is a StreamStmt using a Stream.
type StreamStmt struct {
	Stream   *stream.Stream
	ReadOnly bool
}

// Run returns a result containing the stream. The stream will be executed by calling the Iterate method of
// the result.
func (s *StreamStmt) Prepare(ctx *Context) (Statement, error) {
	st, err := planner.Optimize(s.Stream, ctx.Catalog)
	if err != nil {
		return nil, err
	}

	return &PreparedStreamStmt{
		Stream:   st,
		ReadOnly: s.ReadOnly,
	}, nil
}

// PreparedStreamStmt is a PreparedStreamStmt using a Stream.
type PreparedStreamStmt struct {
	Stream   *stream.Stream
	ReadOnly bool
}

// Run returns a result containing the stream. The stream will be executed by calling the Iterate method of
// the result.
func (s *PreparedStreamStmt) Run(ctx *Context) (Result, error) {
	return Result{
		Iterator: &StreamStmtIterator{
			Stream:  s.Stream,
			Context: ctx,
		},
	}, nil
}

// IsReadOnly reports whether the stream will modify the database or only read it.
func (s *PreparedStreamStmt) IsReadOnly() bool {
	return s.ReadOnly
}

func (s *PreparedStreamStmt) String() string {
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
