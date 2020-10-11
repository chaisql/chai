package fuzz

import (
	"context"
	"strings"

	"github.com/genjidb/genji/sql/parser"
)

func FuzzParseQuery(data []byte) int {
	var b strings.Builder
	b.Write(data)
	q, err := parser.ParseQuery(context.Background(), b.String())
	if err != nil {
		return 0
	}
	if len(q.Statements) != 0 {
		return 1
	}
	return 0
}
