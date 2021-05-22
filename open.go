// +build !wasm

package genji

import (
	"context"
	"strings"

	"github.com/genjidb/genji/database"
	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/engine"
	"github.com/genjidb/genji/engine/boltengine"
	"github.com/genjidb/genji/engine/memoryengine"
	"github.com/genjidb/genji/query"
	"github.com/genjidb/genji/sql/parser"
)

// Open creates a Genji database at the given path.
// If path is equal to ":memory:" it will open an in-memory database,
// otherwise it will create an on-disk database using the BoltDB engine.
func Open(path string) (*DB, error) {
	var ng engine.Engine
	var err error

	switch path {
	case ":memory:":
		ng = memoryengine.NewEngine()
	default:
		ng, err = boltengine.NewEngine(path, 0660, nil)
	}
	if err != nil {
		return nil, err
	}

	ctx := context.Background()
	return New(ctx, ng)
}

func newDatabase(ctx context.Context, ng engine.Engine, opts database.Options) (*DB, error) {
	db, err := database.New(ctx, ng, opts)
	if err != nil {
		return nil, err
	}

	tx, err := db.Begin(true)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	err = loadCatalog(tx)
	if err != nil {
		return nil, err
	}

	return &DB{
		DB:  db,
		ctx: context.Background(),
	}, nil
}

func loadCatalog(tx *database.Transaction) error {
	tables, err := loadCatalogTables(tx)
	if err != nil {
		return err
	}

	indexes, err := loadCatalogIndexes(tx)
	if err != nil {
		return err
	}

	tx.Catalog.Load(tables, indexes)
	return nil
}

func loadCatalogTables(tx *database.Transaction) ([]database.TableInfo, error) {
	tb := database.GetTableStore(tx)

	var tables []database.TableInfo
	err := tb.AscendGreaterOrEqual(document.Value{}, func(d document.Document) error {
		s, err := d.GetByField("sql")
		if err != nil {
			return err
		}

		stmt, err := parser.NewParser(strings.NewReader(s.V.(string))).ParseStatement()
		if err != nil {
			return err
		}

		ti := stmt.(query.CreateTableStmt).Info

		v, err := d.GetByField("store_name")
		if err != nil {
			return err
		}
		ti.StoreName = v.V.([]byte)

		tables = append(tables, ti)
		return nil
	})
	if err != nil {
		return nil, err
	}

	return tables, nil
}

func loadCatalogIndexes(tx *database.Transaction) ([]database.IndexInfo, error) {
	tb := database.GetIndexStore(tx)

	var indexes []database.IndexInfo
	err := tb.AscendGreaterOrEqual(document.Value{}, func(d document.Document) error {
		s, err := d.GetByField("sql")
		if err != nil {
			return err
		}

		stmt, err := parser.NewParser(strings.NewReader(s.V.(string))).ParseStatement()
		if err != nil {
			return err
		}

		indexes = append(indexes, stmt.(query.CreateIndexStmt).Info)
		return nil
	})

	return indexes, err
}
