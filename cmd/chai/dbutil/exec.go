package dbutil

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"

	"github.com/chaisql/chai/internal/sql/parser"
)

// ExecSQL reads SQL queries from reader and executes them until the reader is exhausted.
// If the query has results, they will be outputted to w.
func ExecSQL(ctx context.Context, db *sql.DB, r io.Reader, w io.Writer) error {
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")

	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()

	return parser.NewParser(r).ParseRaw(func(q string) error {
		rows, err := conn.QueryContext(ctx, q)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return nil
			}
			return err
		}
		defer rows.Close()

		for rows.Next() {
			cols, err := rows.Columns()
			if err != nil {
				return err
			}
			values := make([]any, len(cols))
			valuesPtr := make([]any, len(cols))
			for i := range values {
				valuesPtr[i] = &values[i]
			}
			if err := rows.Scan(valuesPtr...); err != nil {
				return err
			}
			err = enc.Encode(valuesPtr)
			if err != nil {
				return err
			}
		}

		return rows.Err()
	})
}
