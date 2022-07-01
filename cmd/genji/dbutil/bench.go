package dbutil

import (
	"encoding/csv"
	"encoding/json"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/genjidb/genji"
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
	Prepare(q string) (*genji.Statement, error)
}

type execer func(q string, args ...interface{}) error

// Bench takes a database and dumps its content as SQL queries in the given writer.
// If tables is provided, only selected tables will be outputted.
func Bench(db *genji.DB, query string, opt BenchOptions) error {
	var p preparer = db
	var e execer = db.Exec

	if opt.SameTx {
		tx, err := db.Begin(true)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		p = tx
		e = tx.Exec
	}

	if opt.Init != "" {
		err := e(opt.Init)
		if err != nil {
			return err
		}
	}

	if opt.Prepare {
		stmt, err := p.Prepare(query)
		if err != nil {
			return err
		}
		e = func(q string, args ...interface{}) error {
			return stmt.Exec()
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

			err := e(query)
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
