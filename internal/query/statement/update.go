package statement

import (
	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/internal/expr"
	"github.com/genjidb/genji/internal/stream"
)

// UpdateConfig holds UPDATE configuration.
type UpdateStmt struct {
	TableName string

	// SetPairs is used along with the Set clause. It holds
	// each path with its corresponding value that
	// should be set in the document.
	SetPairs []UpdateSetPair

	// UnsetFields is used along with the Unset clause. It holds
	// each path that should be unset from the document.
	UnsetFields []string

	WhereExpr expr.Expr
}

type UpdateSetPair struct {
	Path document.Path
	E    expr.Expr
}

// ToTree turns the statement into a stream.
func (stmt *UpdateStmt) ToStream() *StreamStmt {
	s := stream.New(stream.SeqScan(stmt.TableName))

	if stmt.WhereExpr != nil {
		s = s.Pipe(stream.Filter(stmt.WhereExpr))
	}

	if stmt.SetPairs != nil {
		for _, pair := range stmt.SetPairs {
			s = s.Pipe(stream.Set(pair.Path, pair.E))
		}
	} else if stmt.UnsetFields != nil {
		for _, name := range stmt.UnsetFields {
			s = s.Pipe(stream.Unset(name))
		}
	}

	s = s.Pipe(stream.TableReplace(stmt.TableName))

	return &StreamStmt{
		Stream:   s,
		ReadOnly: false,
	}
}
