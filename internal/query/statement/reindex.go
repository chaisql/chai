package statement

import (
	errs "github.com/genjidb/genji/errors"
	"github.com/genjidb/genji/internal/database"
	"github.com/genjidb/genji/internal/stream"
)

// ReIndexStmt is a DSL that allows creating a full REINDEX statement.
type ReIndexStmt struct {
	TableOrIndexName string
}

// IsReadOnly always returns false. It implements the Statement interface.
func (stmt ReIndexStmt) IsReadOnly() bool {
	return false
}

// Run runs the Reindex statement in the given transaction.
// It implements the Statement interface.
func (stmt ReIndexStmt) Run(ctx *Context) (Result, error) {
	var res Result

	var indexNames []string

	if stmt.TableOrIndexName == "" {
		indexNames = ctx.Catalog.Cache.ListObjects(database.RelationIndexType)
	} else if _, err := ctx.Catalog.GetTable(ctx.Tx, stmt.TableOrIndexName); err == nil {
		indexNames = ctx.Catalog.ListIndexes(stmt.TableOrIndexName)
	} else if !errs.IsNotFoundError(err) {
		return res, err
	} else {
		indexNames = []string{stmt.TableOrIndexName}
	}

	var streams []*stream.Stream

	for _, indexName := range indexNames {
		idx, err := ctx.Catalog.GetIndex(ctx.Tx, indexName)
		if err != nil {
			return res, err
		}

		err = idx.Truncate()
		if err != nil {
			return res, err
		}

		s := stream.New(stream.SeqScan(idx.Info.TableName)).Pipe(stream.IndexInsert(idx.Info.IndexName))
		streams = append(streams, s)
	}

	ss := StreamStmt{
		Stream:   stream.New(stream.Concat(streams...)),
		ReadOnly: false,
	}

	return ss.Run(ctx)
}
