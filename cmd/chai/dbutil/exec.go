package dbutil

import (
	"bytes"
	"context"
	"database/sql"
	"io"
	"strings"

	"github.com/chaisql/chai/internal/database"
	"github.com/chaisql/chai/internal/query"
	"github.com/chaisql/chai/internal/query/statement"
	"github.com/chaisql/chai/internal/sql/driver"
	"github.com/chaisql/chai/internal/sql/parser"
	"github.com/chaisql/chai/internal/types"
)

// ExecSQL reads SQL queries from reader and executes them until the reader is exhausted.
// If the query has results, they will be outputted to w.
func ExecSQL(ctx context.Context, db *sql.DB, r io.Reader, w io.Writer) error {
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()

	var buf bytes.Buffer

	return conn.Raw(func(driverConn any) error {
		conn := driverConn.(*driver.Conn)

		var stmtWithOutputCount int
		return parser.NewParser(r).Parse(func(s statement.Statement) error {
			res, err := query.New(s).Run(&query.Context{
				Ctx:  ctx,
				DB:   conn.DB(),
				Conn: conn.Conn(),
			})
			if err != nil {
				return err
			}
			defer res.Close()

			cols, err := res.Columns()
			if err != nil {
				return err
			}

			var headerPrinted bool

			return res.Iterate(func(r database.Row) error {
				buf.Reset()
				if err := ctx.Err(); err != nil {
					return err
				}

				if !headerPrinted {
					if stmtWithOutputCount > 0 {
						buf.WriteString("\n")
					}
					stmtWithOutputCount++

					buf.WriteString(strings.Join(cols, "|"))
					buf.WriteString("\n")
					headerPrinted = true
				}

				var i int
				err = r.Iterate(func(column string, value types.Value) error {
					if i > 0 {
						buf.WriteString("|")
					}
					if value == nil {
						buf.WriteString("NULL")
					} else {
						buf.WriteString(value.String())
					}
					i++
					return nil
				})
				if err != nil {
					return err
				}

				buf.WriteString("\n")

				_, err = w.Write(buf.Bytes())
				return err
			})
		})
	})
}
