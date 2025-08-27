package dbutil

import (
	"context"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"io"
	"os"
	"strconv"
	"strings"
	"time"
)

type BenchOptions struct {
	Init       string
	N          int
	SampleSize int
	SameTx     bool
	Prepare    bool
	CSV        bool
}

type preparer interface {
	PrepareContext(ctx context.Context, q string) (*sql.Stmt, error)
}

type execer func(query string, args ...any) (sql.Result, error)

// Bench takes a database and dumps its content as SQL queries in the given writer.
// If tables is provided, only selected tables will be outputted.
func Bench(ctx context.Context, db *sql.DB, query string, opt BenchOptions) error {
	var tx *sql.Tx
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()

	var p preparer = conn
	var e execer = db.Exec

	if opt.SameTx {
		tx, err = conn.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		p = tx
		e = tx.Exec
	}

	if opt.Init != "" {
		_, err := e(opt.Init)
		if err != nil {
			return err
		}
	}

	if opt.Prepare {
		stmt, err := p.PrepareContext(ctx, query)
		if err != nil {
			return err
		}
		e = func(query string, args ...any) (sql.Result, error) {
			return stmt.ExecContext(ctx, args...)
		}
	}

	var enc encoder
	if opt.CSV {
		enc = newCSVWriter(os.Stdout)
	} else {
		enc = newJSONWriter(os.Stdout)
	}

	var totalDuration time.Duration
	for i := 0; i < opt.N; i += opt.SampleSize {
		var total time.Duration

		for j := 0; j < opt.SampleSize; j++ {
			start := time.Now()

			_, err := e(query)
			total += time.Since(start)
			if err != nil {
				return err
			}
		}

		totalDuration += total
		avg := total / time.Duration(opt.SampleSize)
		qps := int(time.Second / avg)

		err := enc(map[string]interface{}{
			"totalQueries":     i + opt.SampleSize,
			"averageDuration":  avg,
			"queriesPerSecond": qps,
			"totalDuration":    totalDuration,
		})
		if err != nil {
			return err
		}
	}

	if opt.SameTx {
		err = tx.Commit()
		if err != nil {
			return err
		}
	}

	return nil
}

type encoder func(map[string]interface{}) error

func newJSONWriter(w io.Writer) func(map[string]interface{}) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return func(m map[string]interface{}) error {
		return enc.Encode(m)
	}
}

func newCSVWriter(w io.Writer) func(map[string]interface{}) error {
	enc := csv.NewWriter(w)
	enc.Comma = ';'
	header := []string{"totalQueries", "averageDuration", "queriesPerSecond", "totalDuration"}
	var headerWritten bool

	return func(m map[string]interface{}) error {
		if !headerWritten {
			err := enc.Write(header)
			if err != nil {
				return err
			}
			headerWritten = true
		}
		err := enc.Write([]string{
			strconv.Itoa(m["totalQueries"].(int)),
			durationToString(m["averageDuration"].(time.Duration)),
			strconv.Itoa(m["queriesPerSecond"].(int)),
			durationToString(m["totalDuration"].(time.Duration)),
		})
		if err != nil {
			return err
		}
		enc.Flush()
		return enc.Error()
	}
}

func durationToMilliseconds(d time.Duration) float64 {
	m := d / time.Millisecond
	nsec := d % time.Millisecond
	return float64(m) + float64(nsec)/1e6
}

func durationToString(d time.Duration) string {
	ms := durationToMilliseconds(d)
	return strings.Replace(strconv.FormatFloat(ms, 'f', -1, 64), ".", ",", 1)
}
