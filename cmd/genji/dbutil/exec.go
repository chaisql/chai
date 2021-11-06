package dbutil

import (
	"context"
	"encoding/json"
	"io"

	"github.com/genjidb/genji"
	"github.com/genjidb/genji/internal/query"
	"github.com/genjidb/genji/internal/sql/parser"
	"github.com/genjidb/genji/types"
)

// ExecSQL reads SQL queries from reader and executes them until the reader is exhausted.
// If the query has results, they will be outputted to w.
func ExecSQL(ctx context.Context, db *genji.DB, r io.Reader, w io.Writer) error {
	q, err := parser.NewParser(r).ParseQuery()
	if err != nil {
		return err
	}

	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")

	for _, stmt := range q.Statements {
		qq := query.New(stmt)
		qctx := query.Context{
			Ctx: ctx,
			DB:  db.DB,
		}
		err = qq.Prepare(&qctx)
		if err != nil {
			return err
		}

		res, err := qq.Run(&qctx)
		if err != nil {
			return err
		}

		err = res.Iterate(func(d types.Document) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			return enc.Encode(d)
		})
		if err != nil {
			res.Close()
			return err
		}

		err = res.Close()
		if err != nil {
			return err
		}
	}

	return nil
}
