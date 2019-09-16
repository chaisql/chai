package sql

import (
	"context"
	"database/sql/driver"
	"errors"
	"io"
	"sync"

	"github.com/asdine/genji"
	"github.com/asdine/genji/query"
	"github.com/asdine/genji/record"
)

// Conn represents a connection to the Genji database.
// It implements the database/sql/driver.Conn interface.
type Conn struct {
	db *genji.DB
}

// Prepare returns a prepared statement, bound to this connection.
func (c Conn) Prepare(q string) (driver.Stmt, error) {
	stmt, err := query.ParseStatement(q)
	if err != nil {
		return nil, err
	}

	return Stmt{
		txo:  &query.TxOpener{DB: c.db},
		stmt: stmt,
	}, nil
}

// Close does nothing.
func (c Conn) Close() error {
	return nil
}

// Begin starts and returns a new transaction.
func (c Conn) Begin() (driver.Tx, error) {
	return c.db.Begin(true)
}

// BeginTx starts and returns a new transaction.
// It uses the ReadOnly option to determine whether to start a read-only or read/write transaction.
// If the Isolation option is non zero, an error is returned.
func (c Conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if opts.Isolation != 0 {
		return nil, errors.New("isolation levels are not supported")
	}

	return c.db.Begin(!opts.ReadOnly)
}

// Stmt is a prepared statement. It is bound to a Conn and not
// used by multiple goroutines concurrently.
type Stmt struct {
	txo  *query.TxOpener
	stmt query.Statement
}

// NumInput returns the number of placeholder parameters.
func (s Stmt) NumInput() int { return -1 }

// Exec executes a query that doesn't return rows, such
// as an INSERT or UPDATE.
//
// Deprecated: Drivers should implement StmtExecContext instead (or additionally).
func (s Stmt) Exec(args []driver.Value) (driver.Result, error) { return nil, nil }

// ExecContext executes a query that doesn't return rows, such
// as an INSERT or UPDATE.
//
// ExecContext must honor the context timeout and return when it is canceled.
func (s Stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	res := s.stmt.Run(s.txo)

	return res, res.Err()
}

// Query executes a query that may return rows, such as a
// SELECT.
func (s Stmt) Query(args []driver.Value) (driver.Rows, error) {
	res := s.stmt.Run(s.txo)

	if err := res.Err(); err != nil {
		return nil, err
	}

	return newRecordStream(res), nil
}

// Close does nothing.
func (s Stmt) Close() error {
	return nil
}

type recordStream struct {
	res      query.Result
	cancelFn func()
	c        chan rec
	wg       sync.WaitGroup
}

type rec struct {
	recordID []byte
	r        record.Record
	err      error
}

func newRecordStream(res query.Result) *recordStream {
	ctx, cancel := context.WithCancel(context.Background())

	c := make(chan rec)

	records := recordStream{
		res:      res,
		cancelFn: cancel,
		c:        c,
	}
	records.wg.Add(1)

	go records.iterate(ctx)

	return &records
}

var errStop = errors.New("stop")

func (rs *recordStream) iterate(ctx context.Context) {
	defer rs.wg.Done()

	err := rs.res.Iterate(func(recordID []byte, r record.Record) error {
		select {
		case <-ctx.Done():
			return errStop
		case rs.c <- rec{
			recordID: recordID,
			r:        r,
		}:
		}

		return nil
	})

	if err == errStop {
		return
	}
	if err != nil {
		rs.c <- rec{
			err: err,
		}
		return
	}

	if err == nil {
		rs.c <- rec{
			err: io.EOF,
		}
	}
}

// Columns always returns one column named "record".
func (rs *recordStream) Columns() []string {
	return []string{"record"}
}

// Close closes the rows iterator.
func (rs *recordStream) Close() error { return nil }

// Next expects exactly one destination. This destination must implement record.Scanner
// otherwise an error is returned.
func (rs *recordStream) Next(dest []driver.Value) error {
	rec := <-rs.c
	if rec.err != nil {
		return rec.err
	}

	scanner, ok := dest[0].(record.Scanner)
	if !ok {
		return errors.New("destination must implement record.Scanner")
	}

	return scanner.ScanRecord(rec.r)
}
