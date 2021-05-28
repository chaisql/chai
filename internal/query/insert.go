package query

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
	SelectStmt *SelectStmt
	Returning  []expr.Expr
	OnConflict database.OnInsertConflictAction
}

func (stmt *InsertStmt) Run(tx *database.Transaction, params []expr.Param) (Result, error) {
	var res Result

	s, err := stmt.ToStream()
	if err != nil {
		return res, err
	}

	return s.Run(tx, params)
}

func (stmt *InsertStmt) IsReadOnly() bool {
	return false
}

func (stmt *InsertStmt) ToStream() (*StreamStmt, error) {
	var s *stream.Stream

	if stmt.Values != nil {
		s = stream.New(stream.Expressions(stmt.Values...))
	} else {
		st, err := stmt.SelectStmt.ToStream()
		if err != nil {
			return nil, err
		}
		s = st.Stream

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
