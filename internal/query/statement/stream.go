package statement

import (
	"github.com/chaisql/chai/internal/database"
	"github.com/chaisql/chai/internal/environment"
	"github.com/chaisql/chai/internal/planner"
	"github.com/chaisql/chai/internal/stream"
)

var _ Statement = (*PreparedStreamStmt)(nil)

// StreamStmt is a StreamStmt using a Stream.
type StreamStmt struct {
	Stream   *stream.Stream
	ReadOnly bool
}

// PreparedStreamStmt is a PreparedStreamStmt using a Stream.
type PreparedStreamStmt struct {
	Stream *stream.Stream
}

// Run returns a result containing the stream. The stream will be executed by calling the Iterate method of
// the result.
func (s *PreparedStreamStmt) Run(ctx *Context) (*Result, error) {
	st, err := planner.Optimize(s.Stream.Clone(), ctx.Conn.GetTx().Catalog, ctx.Params)
	if err != nil {
		return nil, err
	}

	return &Result{
		Result: &StreamStmtResult{
			Stream:  st,
			Context: ctx,
		},
	}, nil
}

func (s *PreparedStreamStmt) String() string {
	return s.Stream.String()
}

// StreamStmtResult iterates over a stream.
type StreamStmtResult struct {
	Stream  *stream.Stream
	Context *Context
}

func (s *StreamStmtResult) Iterator() (database.Iterator, error) {
	env := environment.New(s.Context.DB, s.Context.Conn.GetTx(), s.Context.Params, nil)

	return s.Stream.Iterator(env)
}
