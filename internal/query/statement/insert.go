package statement

import (
	"errors"

	"github.com/genjidb/genji/internal/database"
	"github.com/genjidb/genji/internal/expr"
	"github.com/genjidb/genji/internal/stream"
)

// InsertStmt holds INSERT configuration.
type InsertStmt struct {
	TableName  string
	Values     []expr.Expr
	Fields     []string
	SelectStmt *StreamStmt
	Returning  []expr.Expr
	OnConflict database.OnInsertConflictAction
}

func (stmt *InsertStmt) ToStream() (*StreamStmt, error) {
	var s *stream.Stream

	if stmt.Values != nil {
		s = stream.New(stream.Expressions(stmt.Values...))
	} else {
		s = stmt.SelectStmt.Stream

		// ensure we are not reading and writing to the same table.
		if s.First().(*stream.SeqScanOperator).TableName == stmt.TableName {
			return nil, errors.New("cannot read and write to the same table")
		}

		if len(stmt.Fields) > 0 {
			s = s.Pipe(stream.IterRename(stmt.Fields...))
		}
	}

	s = s.Pipe(stream.TableInsert(stmt.TableName, stmt.OnConflict))

	if len(stmt.Returning) > 0 {
		s = s.Pipe(stream.Project(stmt.Returning...))
	}

	return &StreamStmt{
		Stream:   s,
		ReadOnly: false,
	}, nil
}
