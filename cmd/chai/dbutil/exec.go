package dbutil

import (
	"context"
	"encoding/json"
	"io"

	"github.com/chaisql/chai"
	"github.com/chaisql/chai/internal/database"
	"github.com/chaisql/chai/internal/query"
	"github.com/chaisql/chai/internal/query/statement"
	"github.com/chaisql/chai/internal/sql/parser"
)

// ExecSQL reads SQL queries from reader and executes them until the reader is exhausted.
// If the query has results, they will be outputted to w.
func ExecSQL(ctx context.Context, db *chai.DB, r io.Reader, w io.Writer) error {
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")

	conn, err := db.Connect()
	if err != nil {
		return err
	}
	defer conn.Close()

	return parser.NewParser(r).Parse(func(s statement.Statement) error {
		qq := query.New(s)
		qctx := query.Context{
			Ctx:  ctx,
			DB:   db.DB,
			Conn: conn.Conn,
		}
		err := qq.Prepare(&qctx)
		if err != nil {
			return err
		}

		res, err := qq.Run(&qctx)
		if err != nil {
			return err
		}

		err = res.Iterate(func(r database.Row) error {
			if err := ctx.Err(); err != nil {
				return err
			}

			return enc.Encode(r)
		})
		if err != nil {
			res.Close()
			return err
		}

		return res.Close()
	})
}
