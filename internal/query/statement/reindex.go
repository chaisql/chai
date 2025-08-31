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
	PreparedStreamStmt

	TableOrIndexName string
}

// Prepare implements the Preparer interface.
func (stmt *ReIndexStmt) Prepare(ctx *Context) (Statement, error) {
	var indexNames []string

	if stmt.TableOrIndexName == "" {
		indexNames = ctx.Conn.GetTx().Catalog.Cache.ListObjects(database.RelationIndexType)
	} else if _, err := ctx.Conn.GetTx().Catalog.GetTable(ctx.Conn.GetTx(), stmt.TableOrIndexName); err == nil {
		indexNames = ctx.Conn.GetTx().Catalog.ListIndexes(stmt.TableOrIndexName)
	} else if !errs.IsNotFoundError(err) {
		return nil, err
	} else {
		indexNames = []string{stmt.TableOrIndexName}
	}

	var streams []*stream.Stream

	for _, indexName := range indexNames {
		idx, err := ctx.Conn.GetTx().Catalog.GetIndex(ctx.Conn.GetTx(), indexName)
		if err != nil {
			return nil, err
		}
		info, err := ctx.Conn.GetTx().Catalog.GetIndexInfo(indexName)
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

	s := stream.New(stream.Concat(streams...)).Pipe(stream.Discard())

	stmt.PreparedStreamStmt.Stream = s
	return stmt, nil
}
