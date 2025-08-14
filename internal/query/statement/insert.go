package statement

import (
	"github.com/chaisql/chai/internal/database"
	"github.com/chaisql/chai/internal/expr"
	"github.com/chaisql/chai/internal/stream"
	"github.com/chaisql/chai/internal/stream/index"
	"github.com/chaisql/chai/internal/stream/path"
	"github.com/chaisql/chai/internal/stream/rows"
	"github.com/chaisql/chai/internal/stream/table"
	"github.com/cockroachdb/errors"
)

var _ Statement = (*InsertStmt)(nil)

// InsertStmt holds INSERT configuration.
type InsertStmt struct {
	basePreparedStatement

	TableName  string
	Values     []expr.Expr
	Columns    []string
	SelectStmt Preparer
	Returning  []expr.Expr
	OnConflict database.OnConflictAction
}

func NewInsertStatement() *InsertStmt {
	var p InsertStmt

	p.basePreparedStatement = basePreparedStatement{
		Preparer: &p,
		ReadOnly: false,
	}

	return &p
}

func (stmt *InsertStmt) Bind(ctx *Context) error {
	for i := range stmt.Values {
		err := BindExpr(ctx, stmt.TableName, stmt.Values[i])
		if err != nil {
			return err
		}
	}

	if stmt.SelectStmt != nil {
		if s, ok := stmt.SelectStmt.(Statement); ok {
			err := s.Bind(ctx)
			if err != nil {
				return err
			}
		}
	}

	for i := range stmt.Returning {
		err := BindExpr(ctx, stmt.TableName, stmt.Returning[i])
		if err != nil {
			return err
		}
	}

	return nil
}

func (stmt *InsertStmt) Prepare(c *Context) (Statement, error) {
	var s *stream.Stream

	var columns []string
	if stmt.Values != nil {
		ti, err := c.Tx.Catalog.GetTableInfo(stmt.TableName)
		if err != nil {
			return nil, err
		}

		var rowList []expr.Row
		// if no columns have been specified, we need to inject the columns from the defined table info
		if len(stmt.Columns) == 0 {

			rowList = make([]expr.Row, 0, len(stmt.Values))
			for i := range stmt.Values {
				var r expr.Row
				var ok bool

				r.Exprs, ok = stmt.Values[i].(expr.LiteralExprList)
				if !ok {
					continue
				}

				for i := range r.Exprs {
					r.Columns = append(r.Columns, ti.ColumnConstraints.Ordered[i].Column)
				}

				columns = r.Columns

				rowList = append(rowList, r)
			}
		} else {
			columns = stmt.Columns

			rowList = make([]expr.Row, 0, len(stmt.Values))
			for i := range stmt.Columns {
				_, ok := ti.ColumnConstraints.ByColumn[stmt.Columns[i]]
				if !ok {
					return nil, errors.Errorf("table has no column %s", stmt.Columns[i])
				}
			}

			for i := range stmt.Values {
				var r expr.Row
				var ok bool

				r.Exprs, ok = stmt.Values[i].(expr.LiteralExprList)
				if !ok {
					continue
				}

				r.Columns = stmt.Columns
				if len(stmt.Columns) != len(r.Exprs) {
					return nil, errors.Errorf("expected %d columns, got %d", len(stmt.Columns), len(stmt.Values))
				}
				rowList = append(rowList, r)
			}
		}

		s = stream.New(rows.Emit(columns, rowList...))
	} else {
		selectStream, err := stmt.SelectStmt.Prepare(c)
		if err != nil {
			return nil, err
		}

		s = selectStream.(*PreparedStreamStmt).Stream

		// ensure we are not reading and writing to the same table.
		// TODO(asdine): if same table, write content to a temp table.
		if tableScan, ok := s.First().(*table.ScanOperator); ok && tableScan.TableName == stmt.TableName {
			return nil, errors.New("cannot read and write to the same table")
		}

		if len(stmt.Columns) > 0 {
			s = s.Pipe(path.PathsRename(stmt.Columns...))
		}
	}

	// validate object
	s = s.Pipe(table.Validate(stmt.TableName))

	if stmt.OnConflict != 0 {
		switch stmt.OnConflict {
		case database.OnConflictDoNothing:
			s = s.Pipe(stream.OnConflict(nil))
		case database.OnConflictDoReplace:
			s = s.Pipe(stream.OnConflict(stream.New(table.Replace(stmt.TableName))))
		default:
			panic("unreachable")
		}
	}

	// check unique constraints
	indexNames := c.Tx.Catalog.ListIndexes(stmt.TableName)
	for _, indexName := range indexNames {
		info, err := c.Tx.Catalog.GetIndexInfo(indexName)
		if err != nil {
			return nil, err
		}

		if info.Unique {
			s = s.Pipe(index.Validate(indexName))
		}
	}

	s = s.Pipe(table.Insert(stmt.TableName))

	for _, indexName := range indexNames {
		s = s.Pipe(index.Insert(indexName))
	}

	if len(stmt.Returning) > 0 {
		s = s.Pipe(rows.Project(stmt.Returning...))
	} else {
		s = s.Pipe(stream.Discard())
	}

	st := StreamStmt{
		Stream:   s,
		ReadOnly: false,
	}

	return st.Prepare(c)
}
