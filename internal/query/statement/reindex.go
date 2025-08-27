package statement

import (
	"github.com/chaisql/chai/internal/database"
	errs "github.com/chaisql/chai/internal/errors"
	"github.com/chaisql/chai/internal/stream"
	"github.com/chaisql/chai/internal/stream/index"
	"github.com/chaisql/chai/internal/stream/table"
)

var _ Statement = (*ReIndexStmt)(nil)

// ReIndexStmt is a DSL that allows creating a full REINDEX statement.
type ReIndexStmt struct {
	basePreparedStatement

	TableOrIndexName string
}

func NewReIndexStatement() *ReIndexStmt {
	var p ReIndexStmt

	p.basePreparedStatement = basePreparedStatement{
		Preparer: &p,
		ReadOnly: false,
	}

	return &p
}

func (stmt *ReIndexStmt) Bind(ctx *Context) error {
	return nil
}

// Prepare implements the Preparer interface.
func (stmt *ReIndexStmt) Prepare(ctx *Context) (Statement, error) {
	var indexNames []string

	if stmt.TableOrIndexName == "" {
		indexNames = ctx.Tx.Catalog.Cache.ListObjects(database.RelationIndexType)
	} else if _, err := ctx.Tx.Catalog.GetTable(ctx.Tx, stmt.TableOrIndexName); err == nil {
		indexNames = ctx.Tx.Catalog.ListIndexes(stmt.TableOrIndexName)
	} else if !errs.IsNotFoundError(err) {
		return nil, err
	} else {
		indexNames = []string{stmt.TableOrIndexName}
	}

	var streams []*stream.Stream

	for _, indexName := range indexNames {
		idx, err := ctx.Tx.Catalog.GetIndex(ctx.Tx, indexName)
		if err != nil {
			return nil, err
		}
		info, err := ctx.Tx.Catalog.GetIndexInfo(indexName)
		if err != nil {
			return nil, err
		}

		err = idx.Truncate()
		if err != nil {
			return nil, err
		}

		s := stream.New(table.Scan(info.Owner.TableName)).Pipe(index.Insert(info.IndexName))
		streams = append(streams, s)
	}

	st := StreamStmt{
		Stream:   stream.New(stream.Concat(streams...)).Pipe(stream.Discard()),
		ReadOnly: false,
	}

	return st.Prepare(ctx)
}
