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

// InsertStmt holds INSERT configuration.
type InsertStmt struct {
	basePreparedStatement

	TableName  string
	Values     []expr.Expr
	Fields     []string
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

func (stmt *InsertStmt) Prepare(c *Context) (Statement, error) {
	var s *stream.Stream

	if stmt.Values != nil {
		ti, err := c.Tx.Catalog.GetTableInfo(stmt.TableName)
		if err != nil {
			return nil, err
		}

		// if no fields have been specified, we need to inject the fields from the defined table info
		if len(stmt.Fields) == 0 {
			for i := range stmt.Values {
				kvs, ok := stmt.Values[i].(*expr.KVPairs)
				if !ok {
					continue
				}

				for i := range kvs.Pairs {
					if kvs.Pairs[i].K == "" {
						if i >= len(ti.FieldConstraints.Ordered) {
							return nil, errors.Errorf("too many values for %s", stmt.TableName)
						}

						kvs.Pairs[i].K = ti.FieldConstraints.Ordered[i].Field
					}
				}
			}
		} else {
			if !ti.FieldConstraints.AllowExtraFields {
				for i := range stmt.Fields {
					_, ok := ti.FieldConstraints.ByField[stmt.Fields[i]]
					if !ok {
						return nil, errors.Errorf("table has no field %s", stmt.Fields[i])
					}
				}
			}
		}
		s = stream.New(rows.Emit(stmt.Values...))
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

		if len(stmt.Fields) > 0 {
			s = s.Pipe(path.PathsRename(stmt.Fields...))
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
