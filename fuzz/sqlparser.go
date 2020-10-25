package fuzz

import (
	"strings"

	"github.com/genjidb/genji/sql/parser"
)

func FuzzParseQuery(data []byte) int {
	var b strings.Builder
	b.Write(data)
	q, err := parser.ParseQuery(b.String())
	if err != nil {
		return 0
	}
	if len(q.Statements) != 0 {
		return 1
	}
	return 0
}
