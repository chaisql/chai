package query

import (
	"database/sql/driver"

	"github.com/asdine/genji"
	"github.com/asdine/genji/record"
	"github.com/asdine/genji/table"
)

// A Query can execute statements against the database. It can read or write data
// from any table, or even alter the structure of the database.
// Results are returned as streams.
type Query struct {
	statements []Statement
}

// Run executes all the statements in their own transaction and returns the last result.
func (q Query) Run(db *genji.DB, args ...interface{}) Result {
	txm := TxOpener{DB: db}
	var res Result

	for _, stmt := range q.statements {
		res = stmt.Run(&txm, nil)
		if res.err != nil {
			return res
		}
	}

	return res
}

// New creates a new Query with the given statements.
func New(statements ...Statement) Query {
	return Query{statements: statements}
}

// Run parses s and runs the query against db.
func Run(db *genji.DB, s string) Result {
	q, err := ParseQuery(s)
	if err != nil {
		return Result{err: err}
	}

	return q.Run(db)
}

// A Statement represents a unique action that can be executed against the database.
type Statement interface {
	Run(*TxOpener, []driver.NamedValue) Result
}

// TxOpener is used by statements to automatically open transactions.
// If the Tx field is nil, it will automatically create a new transaction.
// If the Tx field is not nil, it will be passed to View and Update.
type TxOpener struct {
	DB *genji.DB
	Tx *genji.Tx
}

// View runs fn in a read-only transaction if the Tx field is nil.
// If not, it will pass it to fn regardless of it being a read-only or read-write transaction.
func (tx TxOpener) View(fn func(tx *genji.Tx) error) error {
	if tx.Tx != nil {
		return fn(tx.Tx)
	}

	return tx.DB.View(fn)
}

// Update runs fn in a read-write transaction if the Tx field is nil.
// If not, it will pass it to fn regardless of it being a read-only or read-write transaction.
func (tx TxOpener) Update(fn func(tx *genji.Tx) error) error {
	if tx.Tx != nil {
		return fn(tx.Tx)
	}

	return tx.DB.Update(fn)
}

// Result of a query.
type Result struct {
	table.Stream
	rowsAffected       driver.RowsAffected
	err                error
	lastInsertRecordID []byte
}

// Err returns a non nil error if an error occured during the query.
func (r Result) Err() error {
	return r.err
}

// Scan takes a table scanner and passes it the result table.
func (r Result) Scan(s table.Scanner) error {
	if r.err != nil {
		return r.err
	}

	return s.ScanTable(r.Stream)
}

// LastInsertId is not supported and returns an error.
// Use LastInsertRecordID instead.
func (r Result) LastInsertId() (int64, error) {
	return r.rowsAffected.LastInsertId()
}

// LastInsertRecordID returns the database's auto-generated recordID
// after, for example, an INSERT into a table with primary
// key.
func (r Result) LastInsertRecordID() ([]byte, error) {
	return r.lastInsertRecordID, nil
}

// RowsAffected returns the number of rows affected by the
// query.
func (r Result) RowsAffected() (int64, error) {
	return r.rowsAffected.RowsAffected()
}

func whereClause(tx *genji.Tx, e Expr) func(recordID []byte, r record.Record) (bool, error) {
	if e == nil {
		return func(recordID []byte, r record.Record) (bool, error) {
			return true, nil
		}
	}

	return func(recordID []byte, r record.Record) (bool, error) {
		v, err := e.Eval(EvalContext{Tx: tx, Record: r})
		if err != nil {
			return false, err
		}

		return v.Truthy(), nil
	}
}

func argsToNamedValues(args []interface{}) []driver.NamedValue {
	nv := make([]driver.NamedValue, len(args))
	for i := range args {
		switch t := args[i].(type) {
		case driver.NamedValue:
			nv[i] = t
		case *driver.NamedValue:
			nv[i] = *t
		default:
			nv[i].Ordinal = i + 1
			nv[i].Value = args[i]
		}
	}

	return nv
}
