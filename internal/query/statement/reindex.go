package statement

import (
	"github.com/genjidb/genji/internal/database"
	errs "github.com/genjidb/genji/internal/errors"
	"github.com/genjidb/genji/internal/stream"
	"github.com/genjidb/genji/internal/stream/index"
	"github.com/genjidb/genji/internal/stream/table"
)

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

// Prepare implements the Preparer interface.
func (stmt ReIndexStmt) Prepare(ctx *Context) (Statement, error) {
	var indexNames []string

	if stmt.TableOrIndexName == "" {
		indexNames = ctx.Catalog.Cache.ListObjects(database.RelationIndexType)
	} else if _, err := ctx.Catalog.GetTable(ctx.Tx, stmt.TableOrIndexName); err == nil {
		indexNames = ctx.Catalog.ListIndexes(stmt.TableOrIndexName)
	} else if !errs.IsNotFoundError(err) {
		return nil, err
	} else {
		indexNames = []string{stmt.TableOrIndexName}
	}

	var streams []*stream.Stream

	for _, indexName := range indexNames {
		idx, err := ctx.Catalog.GetIndex(ctx.Tx, indexName)
		if err != nil {
			return nil, err
		}
		info, err := ctx.Catalog.GetIndexInfo(indexName)
		if err != nil {
			return nil, err
		}

		err = idx.Truncate()
		if err != nil {
			return nil, err
		}

		s := stream.New(table.Scan(info.Owner.TableName)).Pipe(index.IndexInsert(info.IndexName))
		streams = append(streams, s)
	}

	st := StreamStmt{
		Stream:   stream.New(stream.Concat(streams...)).Pipe(stream.Discard()),
		ReadOnly: false,
	}

	return st.Prepare(ctx)
}
