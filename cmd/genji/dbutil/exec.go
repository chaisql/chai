package dbutil

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"strings"

	"github.com/genjidb/genji"
	"github.com/genjidb/genji/document"
)

// ExecSQL reads SQL queries from reader and executes them until the reader is exhausted.
// If the query has results, they will be outputted to w.
func ExecSQL(ctx context.Context, db *genji.DB, r io.Reader, w io.Writer) error {
	scanner := bufio.NewScanner(r)

	// Every query ends with a semicolon.
	scanner.Split(func(data []byte, atEOF bool) (advance int, token []byte, err error) {
		for i := 0; i < len(data); i++ {
			if data[i] == ';' {
				return i + 1, data[:i], nil
			}
		}

		if !atEOF {
			return 0, nil, nil
		}

		return 0, data, bufio.ErrFinalToken
	})

	for scanner.Scan() {
		q := strings.TrimSpace(scanner.Text())
		if q == "" {
			continue
		}

		if err := runQuery(ctx, db, q, w); err != nil {
			return err
		}
	}

	return scanner.Err()
}

func runQuery(ctx context.Context, db *genji.DB, q string, w io.Writer) error {
	res, err := db.Query(q)
	if err != nil {
		return err
	}
	defer res.Close()

	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")
	return res.Iterate(func(d document.Document) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		return enc.Encode(d)
	})
}
