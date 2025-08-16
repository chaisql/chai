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

// Prepare implements the Preparer interface.
func (s *StreamStmt) Prepare(ctx *Context) (Statement, error) {
	return &PreparedStreamStmt{
		Stream:   s.Stream,
		ReadOnly: s.ReadOnly,
	}, nil
}

// PreparedStreamStmt is a PreparedStreamStmt using a Stream.
type PreparedStreamStmt struct {
	Stream   *stream.Stream
	ReadOnly bool
}

func (s *PreparedStreamStmt) Bind(ctx *Context) error {
	return nil
}

// Run returns a result containing the stream. The stream will be executed by calling the Iterate method of
// the result.
func (s *PreparedStreamStmt) Run(ctx *Context) (Result, error) {
	st, err := planner.Optimize(s.Stream.Clone(), ctx.Tx.Catalog, ctx.Params)
	if err != nil {
		return Result{}, err
	}

	return Result{
		Result: &StreamStmtResult{
			Stream:  st,
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

// StreamStmtResult iterates over a stream.
type StreamStmtResult struct {
	Stream  *stream.Stream
	Context *Context
}

func (s *StreamStmtResult) Iterator() (database.Iterator, error) {
	env := environment.New(s.Context.DB, s.Context.Tx, s.Context.Params, nil)

	return s.Stream.Iterator(env)
}
