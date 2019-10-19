package genji

import (
	"database/sql/driver"
	"errors"

	"github.com/asdine/genji/internal/scanner"
	"github.com/asdine/genji/record"
)

// parseDeleteStatement parses a delete string and returns a Statement AST object.
// This function assumes the DELETE token has already been consumed.
func (p *parser) parseDeleteStatement() (deleteStmt, error) {
	var stmt deleteStmt
	var err error

	// Parse "FROM".
	if tok, pos, lit := p.ScanIgnoreWhitespace(); tok != scanner.FROM {
		return stmt, newParseError(scanner.Tokstr(tok, lit), []string{"FROM"}, pos)
	}

	// Parse table name
	stmt.tableName, err = p.ParseIdent()
	if err != nil {
		return stmt, err
	}

	// Parse condition: "WHERE EXPR".
	stmt.whereExpr, err = p.parseCondition()
	if err != nil {
		return stmt, err
	}

	return stmt, nil
}

// DeleteStmt is a DSL that allows creating a full Delete query.
type deleteStmt struct {
	tableName string
	whereExpr expr
}

// IsReadOnly always returns false. It implements the Statement interface.
func (stmt deleteStmt) IsReadOnly() bool {
	return false
}

func (stmt deleteStmt) Run(tx *Tx, args []driver.NamedValue) (Result, error) {
	var res Result
	if stmt.tableName == "" {
		return res, errors.New("missing table name")
	}

	stack := evalStack{Tx: tx, Params: args}

	t, err := tx.GetTable(stmt.tableName)
	if err != nil {
		return res, err
	}

	st := record.NewStream(t)
	st = st.Filter(whereClause(stmt.whereExpr, stack))

	err = st.Iterate(func(r record.Record) error {
		if k, ok := r.(record.Keyer); ok {
			return t.Delete(k.Key())
		}

		return errors.New("attempt to delete record without key")
	})

	return res, err
}
