package database_test

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/genjidb/genji/document"
	errs "github.com/genjidb/genji/errors"
	"github.com/genjidb/genji/internal/database"
	"github.com/genjidb/genji/internal/query/statement"
	"github.com/genjidb/genji/internal/testutil"
	"github.com/genjidb/genji/internal/testutil/assert"
	"github.com/genjidb/genji/internal/tree"
	"github.com/genjidb/genji/types"
	"github.com/stretchr/testify/require"
)

var errDontCommit = errors.New("don't commit please")

func update(t testing.TB, db *database.Database, fn func(tx *database.Transaction) error) {
	t.Helper()

	tx, err := db.Begin(true)
	assert.NoError(t, err)
	defer tx.Rollback()

	err = fn(tx)
	if errors.Is(err, errDontCommit) {
		tx.Rollback()
		return
	}
	assert.NoError(t, err)

	err = tx.Commit()
	assert.NoError(t, err)
}

func newTestTable(t testing.TB) (*database.Table, func()) {
	t.Helper()

	db, tx, fn := testutil.NewTestTx(t)

	ti := database.TableInfo{TableName: "test"}
	ti.FieldConstraints.AllowExtraFields = true
	return createTable(t, tx, db.Catalog, ti), fn
}

func createTable(t testing.TB, tx *database.Transaction, catalog *database.Catalog, info database.TableInfo) *database.Table {
	stmt := statement.CreateTableStmt{Info: info}

	res, err := stmt.Run(&statement.Context{
		Catalog: catalog,
		Tx:      tx,
	})
	assert.NoError(t, err)
	res.Close()

	tb, err := catalog.GetTable(tx, stmt.Info.TableName)
	assert.NoError(t, err)

	return tb
}

func createTableIfNotExists(t testing.TB, tx *database.Transaction, catalog *database.Catalog, info database.TableInfo) *database.Table {
	t.Helper()

	stmt := statement.CreateTableStmt{Info: info, IfNotExists: true}

	res, err := stmt.Run(&statement.Context{
		Catalog: catalog,
		Tx:      tx,
	})
	assert.NoError(t, err)
	res.Close()

	tb, err := catalog.GetTable(tx, stmt.Info.TableName)
	assert.NoError(t, err)

	return tb
}

func newDocument() *document.FieldBuffer {
	return document.NewFieldBuffer().
		Add("fielda", types.NewTextValue("a")).
		Add("fieldb", types.NewTextValue("b"))
}

// TestTableGetDocument verifies GetDocument behaviour.
func TestTableGetDocument(t *testing.T) {
	t.Run("Should fail if not found", func(t *testing.T) {
		tb, cleanup := newTestTable(t)
		defer cleanup()

		r, err := tb.GetDocument(tree.NewEncodedKey([]byte("id")))
		assert.ErrorIs(t, err, errs.ErrDocumentNotFound)
		require.Nil(t, r)
	})

	t.Run("Should return the right document", func(t *testing.T) {
		tb, cleanup := newTestTable(t)
		defer cleanup()

		// create two documents, one with an additional field
		doc1 := newDocument()
		vc := types.NewDoubleValue(40)
		doc1.Add("fieldc", vc)
		doc2 := newDocument()

		key, _, err := tb.Insert(doc1)
		assert.NoError(t, err)
		_, _, err = tb.Insert(doc2)
		assert.NoError(t, err)

		// fetch doc1 and make sure it returns the right one
		res, err := tb.GetDocument(key)
		assert.NoError(t, err)
		fc, err := res.GetByField("fieldc")
		assert.NoError(t, err)
		ok, err := types.IsEqual(vc, fc)
		assert.NoError(t, err)
		require.True(t, ok)
	})
}

// TestTableInsert verifies Insert behaviour.
func TestTableInsert(t *testing.T) {
	t.Run("Should generate the right docid on existing databases", func(t *testing.T) {
		ng := testutil.NewMemPebble(t)

		db1 := testutil.NewTestDBWithPebble(t, ng)

		insertDoc := func(db *database.Database) (rawKey *tree.Key) {
			t.Helper()

			update(t, db, func(tx *database.Transaction) error {
				t.Helper()

				// create table if not exists
				ti := database.TableInfo{TableName: "test"}
				ti.FieldConstraints.AllowExtraFields = true
				tb := createTableIfNotExists(t, tx, db.Catalog, ti)

				doc := newDocument()
				key, _, err := tb.Insert(doc)
				assert.NoError(t, err)
				require.NotEmpty(t, key)
				rawKey = key
				return nil
			})
			return
		}

		key1 := insertDoc(db1)

		err := db1.Close()
		assert.NoError(t, err)

		// create a new database object
		db2 := testutil.NewTestDBWithPebble(t, ng)

		key2 := insertDoc(db2)

		vs, err := key1.Decode()
		assert.NoError(t, err)
		a := vs[0].V().(int64)

		vs, err = key2.Decode()
		assert.NoError(t, err)
		b := vs[0].V().(int64)

		require.Equal(t, int64(a+1), int64(b))
	})
}

// TestTableDelete verifies Delete behaviour.
func TestTableDelete(t *testing.T) {
	t.Run("Should fail if not found", func(t *testing.T) {
		tb, cleanup := newTestTable(t)
		defer cleanup()

		err := tb.Delete(tree.NewEncodedKey([]byte("id")))
		assert.ErrorIs(t, err, errs.ErrDocumentNotFound)
	})

	t.Run("Should delete the right document", func(t *testing.T) {
		tb, cleanup := newTestTable(t)
		defer cleanup()

		// create two documents, one with an additional field
		doc1 := newDocument()
		doc1.Add("fieldc", types.NewIntegerValue(40))
		doc2 := newDocument()

		key1, _, err := tb.Insert(testutil.CloneDocument(t, doc1))
		assert.NoError(t, err)
		key2, _, err := tb.Insert(testutil.CloneDocument(t, doc2))
		assert.NoError(t, err)

		// delete the document
		err = tb.Delete(key1)
		assert.NoError(t, err)

		// try again, should fail
		err = tb.Delete(key1)
		assert.ErrorIs(t, err, errs.ErrDocumentNotFound)

		// make sure it didn't also delete the other one
		res, err := tb.GetDocument(key2)
		assert.NoError(t, err)
		_, err = res.GetByField("fieldc")
		assert.Error(t, err)
	})
}

// TestTableReplace verifies Replace behaviour.
func TestTableReplace(t *testing.T) {
	t.Run("Should fail if not found", func(t *testing.T) {
		tb, cleanup := newTestTable(t)
		defer cleanup()

		_, err := tb.Replace(tree.NewEncodedKey([]byte("id")), newDocument())
		assert.ErrorIs(t, err, errs.ErrDocumentNotFound)
	})

	t.Run("Should replace the right document", func(t *testing.T) {
		tb, cleanup := newTestTable(t)
		defer cleanup()

		// create two different documents
		doc1 := newDocument()
		doc2 := document.NewFieldBuffer().
			Add("fielda", types.NewTextValue("c")).
			Add("fieldb", types.NewTextValue("d"))

		key1, _, err := tb.Insert(doc1)
		assert.NoError(t, err)
		key2, _, err := tb.Insert(doc2)
		assert.NoError(t, err)

		// create a third document
		doc3 := document.NewFieldBuffer().
			Add("fielda", types.NewTextValue("e")).
			Add("fieldb", types.NewTextValue("f"))

		// replace doc1 with doc3
		d3, err := tb.Replace(key1, doc3)
		assert.NoError(t, err)

		// make sure it replaced it correctly
		res, err := tb.GetDocument(key1)
		assert.NoError(t, err)
		f, err := res.GetByField("fielda")
		assert.NoError(t, err)
		require.Equal(t, "e", f.V().(string))

		testutil.RequireDocEqual(t, d3, res)

		// make sure it didn't also replace the other one
		res, err = tb.GetDocument(key2)
		assert.NoError(t, err)
		f, err = res.GetByField("fielda")
		assert.NoError(t, err)
		require.Equal(t, "c", f.V().(string))
	})
}

// TestTableTruncate verifies Truncate behaviour.
func TestTableTruncate(t *testing.T) {
	t.Run("Should succeed if table empty", func(t *testing.T) {
		tb, cleanup := newTestTable(t)
		defer cleanup()

		err := tb.Truncate()
		assert.NoError(t, err)
	})

	t.Run("Should truncate the table", func(t *testing.T) {
		tb, cleanup := newTestTable(t)
		defer cleanup()

		// create two documents
		doc1 := newDocument()
		doc2 := newDocument()

		_, _, err := tb.Insert(doc1)
		assert.NoError(t, err)
		_, _, err = tb.Insert(doc2)
		assert.NoError(t, err)

		err = tb.Truncate()
		assert.NoError(t, err)

		err = tb.IterateOnRange(nil, false, func(key *tree.Key, _ types.Document) error {
			return errors.New("should not iterate")
		})

		assert.NoError(t, err)
	})
}

// BenchmarkTableInsert benchmarks the Insert method with 1, 10, 1000 and 10000 successive insertions.
func BenchmarkTableInsert(b *testing.B) {
	for size := 1; size <= 10000; size *= 10 {
		b.Run(fmt.Sprintf("%.05d", size), func(b *testing.B) {
			var fb document.FieldBuffer

			for i := int64(0); i < 10; i++ {
				fb.Add(fmt.Sprintf("name-%d", i), types.NewIntegerValue(i))
			}

			b.ResetTimer()
			b.StopTimer()
			for i := 0; i < b.N; i++ {
				tb, cleanup := newTestTable(b)

				b.StartTimer()
				for j := 0; j < size; j++ {
					tb.Insert(&fb)
				}
				b.StopTimer()
				cleanup()
			}
		})
	}
}

// BenchmarkTableScan benchmarks the Scan method with 1, 10, 1000 and 10000 successive insertions.
func BenchmarkTableScan(b *testing.B) {
	for size := 1; size <= 10000; size *= 10 {
		b.Run(fmt.Sprintf("%.05d", size), func(b *testing.B) {
			tb, cleanup := newTestTable(b)
			defer cleanup()

			var fb document.FieldBuffer

			for i := int64(0); i < 10; i++ {
				fb.Add(fmt.Sprintf("name-%d", i), types.NewIntegerValue(i))
			}

			for i := 0; i < size; i++ {
				_, _, err := tb.Insert(&fb)
				assert.NoError(b, err)
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				tb.IterateOnRange(nil, false, func(*tree.Key, types.Document) error {
					return nil
				})
			}
			b.StopTimer()
		})
	}
}
