package genji

import (
	"database/sql/driver"

	"github.com/asdine/genji/record"
)

// A query can execute statements against the database. It can read or write data
// from any table, or even alter the structure of the database.
// Results are returned as streams.
type query struct {
	Statements []statement
}

// Run executes all the statements in their own transaction and returns the last result.
func (q query) Run(db *DB, args []driver.NamedValue) result {
	var res result
	var tx *Tx
	var err error

	for _, stmt := range q.Statements {
		// it there is an opened transaction but there are still statements
		// to be executed, close the current transaction.
		if tx != nil {
			if tx.Writable() {
				err := tx.Commit()
				if err != nil {
					return result{err: err}
				}
			} else {
				err := tx.Rollback()
				if err != nil {
					return result{err: err}
				}
			}
		}

		// start a new transaction for every statement
		tx, err = db.Begin(!stmt.IsReadOnly())
		if err != nil {
			return result{err: err}
		}

		res = stmt.Run(tx, args)
		if res.err != nil {
			tx.Rollback()
			return res
		}
	}

	// the returned result will now own the transaction.
	// its Close method is expected to be called.
	res.tx = tx

	return res
}

// Exec the query within the given transaction. If the one of the statements requires a read-write
// transaction and tx is not, tx will get promoted.
func (q query) Exec(tx *Tx, args []driver.NamedValue, forceReadOnly bool) result {
	var res result

	for _, stmt := range q.Statements {
		// if the statement requires a writable transaction,
		// promote the current transaction.
		if !forceReadOnly && !tx.Writable() && !stmt.IsReadOnly() {
			err := tx.Promote()
			if err != nil {
				return result{err: err}
			}
		}

		res = stmt.Run(tx, args)
		if res.err != nil {
			return res
		}
	}

	return res
}

// newQuery creates a new query with the given statements.
func newQuery(statements ...statement) query {
	return query{Statements: statements}
}

// A statement represents a unique action that can be executed against the database.
type statement interface {
	Run(*Tx, []driver.NamedValue) result
	IsReadOnly() bool
}

// result of a query.
type result struct {
	record.Stream
	rowsAffected       driver.RowsAffected
	err                error
	lastInsertRecordID []byte
	tx                 *Tx
	closed             bool
}

// Err returns a non nil error if an error occured during the query.
func (r result) Err() error {
	return r.err
}

// LastInsertId is not supported and returns an error.
// Use LastInsertRecordID instead.
func (r result) LastInsertId() (int64, error) {
	return r.rowsAffected.LastInsertId()
}

// LastInsertRecordID returns the database's auto-generated recordID
// after, for example, an INSERT into a table with primary
// key.
func (r result) LastInsertRecordID() ([]byte, error) {
	return r.lastInsertRecordID, nil
}

// RowsAffected returns the number of rows affected by the
// query.
func (r result) RowsAffected() (int64, error) {
	return r.rowsAffected.RowsAffected()
}

// Close the result stream. It must be always be called when the
// result is not errored. Calling it when Err() is not nil is safe.
func (r *result) Close() error {
	if r.closed {
		return nil
	}

	r.closed = true

	var err error

	if r.tx != nil {
		if r.tx.Writable() {
			err = r.tx.Commit()
		} else {
			err = r.tx.Rollback()
		}
	}

	return err
}

func whereClause(e expr, stack evalStack) func(r record.Record) (bool, error) {
	if e == nil {
		return func(r record.Record) (bool, error) {
			return true, nil
		}
	}

	return func(r record.Record) (bool, error) {
		stack.Record = r
		v, err := e.Eval(stack)
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

// A fieldSelector can extract a field from a record.
// A Field is an adapter that can turn a string into a field selector.
// It is supposed to be used by casting a string into a Field.
//   f := Field("Name")
//   f.SelectField(r)
// It implements the fieldSelector interface.
type fieldSelector string

// Name returns f as a string.
func (f fieldSelector) Name() string {
	return string(f)
}

// SelectField selects the field f from r.
// SelectField takes a field from a record.
// If the field selector was created using the As method
// it must replace the name of f by the alias.
func (f fieldSelector) SelectField(r record.Record) (record.Field, error) {
	return r.GetField(string(f))
}

// Eval extracts the record from the context and selects the right field.
// It implements the Expr interface.
func (f fieldSelector) Eval(stack evalStack) (evalValue, error) {
	fd, err := f.SelectField(stack.Record)
	if err != nil {
		return nilLitteral, nil
	}

	return newSingleEvalValue(fd.Value), nil
}
