package sql

import (
	"github.com/asdine/genji/query"
	"github.com/asdine/genji/query/sql/parser"
)

func Query(q string) *query.SelectStmt {
	parser.Parse("", []byte(q))
	return nil
}
