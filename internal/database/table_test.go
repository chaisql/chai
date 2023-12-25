package database_test

import (
	"fmt"
	"testing"

	"github.com/chaisql/chai/internal/database"
	"github.com/chaisql/chai/internal/database/catalogstore"
	errs "github.com/chaisql/chai/internal/errors"
	"github.com/chaisql/chai/internal/object"
	"github.com/chaisql/chai/internal/query/statement"
	"github.com/chaisql/chai/internal/testutil"
	"github.com/chaisql/chai/internal/testutil/assert"
	"github.com/chaisql/chai/internal/tree"
	"github.com/chaisql/chai/internal/types"
	"github.com/cockroachdb/errors"
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

	_, tx, fn := testutil.NewTestTx(t)

	ti := database.TableInfo{TableName: "test"}
	ti.FieldConstraints.AllowExtraFields = true
	return createTable(t, tx, ti), fn
}

func createTable(t testing.TB, tx *database.Transaction, info database.TableInfo) *database.Table {
	stmt := statement.CreateTableStmt{Info: info}

	res, err := stmt.Run(&statement.Context{
		Tx: tx,
	})
	assert.NoError(t, err)
	res.Close()

	tb, err := tx.Catalog.GetTable(tx, stmt.Info.TableName)
	assert.NoError(t, err)

	return tb
}

func createTableIfNotExists(t testing.TB, tx *database.Transaction, info database.TableInfo) *database.Table {
	t.Helper()

	stmt := statement.CreateTableStmt{Info: info, IfNotExists: true}

	res, err := stmt.Run(&statement.Context{
		Tx: tx,
	})
	assert.NoError(t, err)
	res.Close()

	tb, err := tx.Catalog.GetTable(tx, stmt.Info.TableName)
	assert.NoError(t, err)

	return tb
}

func newObject() *object.FieldBuffer {
	return object.NewFieldBuffer().
		Add("fielda", types.NewTextValue("a")).
		Add("fieldb", types.NewTextValue("b"))
}

// TestTableGetObject verifies GetObject behaviour.
func TestTableGetObject(t *testing.T) {
	t.Run("Should fail if not found", func(t *testing.T) {
		tb, cleanup := newTestTable(t)
		defer cleanup()

		r, err := tb.GetRow(tree.NewKey(types.NewIntegerValue(10)))
		require.True(t, errs.IsNotFoundError(err))
		require.Nil(t, r)
	})

	t.Run("Should return the right object", func(t *testing.T) {
		tb, cleanup := newTestTable(t)
		defer cleanup()

		// create two objects, one with an additional field
		doc1 := newObject()
		vc := types.NewDoubleValue(40)
		doc1.Add("fieldc", vc)
		doc2 := newObject()

		key, _, err := tb.Insert(doc1)
		assert.NoError(t, err)
		_, _, err = tb.Insert(doc2)
		assert.NoError(t, err)

		// fetch doc1 and make sure it returns the right one
		res, err := tb.GetRow(key)
		assert.NoError(t, err)
		fc, err := res.Get("fieldc")
		assert.NoError(t, err)
		ok, err := types.IsEqual(vc, fc)
		assert.NoError(t, err)
		require.True(t, ok)
	})
}

// TestTableInsert verifies Insert behaviour.
func TestTableInsert(t *testing.T) {
	t.Run("Should generate the right rowid on existing databases", func(t *testing.T) {
		path := t.TempDir()
		db1, err := database.Open(path, &database.Options{
			CatalogLoader: catalogstore.LoadCatalog,
		})
		assert.NoError(t, err)

		insertDoc := func(db *database.Database) (rawKey *tree.Key) {
			t.Helper()

			update(t, db, func(tx *database.Transaction) error {
				t.Helper()

				// create table if not exists
				ti := database.TableInfo{TableName: "test"}
				ti.FieldConstraints.AllowExtraFields = true
				tb := createTableIfNotExists(t, tx, ti)

				doc := newObject()
				key, _, err := tb.Insert(doc)
				assert.NoError(t, err)
				require.NotEmpty(t, key)
				rawKey = key
				return nil
			})
			return
		}

		key1 := insertDoc(db1)

		err = db1.Close()
		assert.NoError(t, err)

		// create a new database object
		db2, err := database.Open(path, &database.Options{
			CatalogLoader: catalogstore.LoadCatalog,
		})
		assert.NoError(t, err)

		key2 := insertDoc(db2)

		vs, err := key1.Decode()
		assert.NoError(t, err)
		a := vs[0].V().(int64)

		vs, err = key2.Decode()
		assert.NoError(t, err)
		b := vs[0].V().(int64)

		require.Equal(t, int64(a+1), int64(b))
		db2.Close()
	})
}

// TestTableDelete verifies Delete behaviour.
func TestTableDelete(t *testing.T) {
	t.Run("Should not fail if not found", func(t *testing.T) {
		tb, cleanup := newTestTable(t)
		defer cleanup()
		err := tb.Delete(tree.NewKey(types.NewIntegerValue(10)))
		assert.NoError(t, err)
	})

	t.Run("Should delete the right object", func(t *testing.T) {
		tb, cleanup := newTestTable(t)
		defer cleanup()

		// create two objects, one with an additional field
		doc1 := newObject()
		doc1.Add("fieldc", types.NewIntegerValue(40))
		doc2 := newObject()

		key1, _, err := tb.Insert(testutil.CloneObject(t, doc1))
		assert.NoError(t, err)
		key2, _, err := tb.Insert(testutil.CloneObject(t, doc2))
		assert.NoError(t, err)

		// delete the object
		err = tb.Delete(key1)
		assert.NoError(t, err)

		// try again, should fail
		_, err = tb.GetRow(key1)
		require.True(t, errs.IsNotFoundError(err))

		// make sure it didn't also delete the other one
		res, err := tb.GetRow(key2)
		assert.NoError(t, err)
		_, err = res.Get("fieldc")
		assert.Error(t, err)
	})
}

// TestTableReplace verifies Replace behaviour.
func TestTableReplace(t *testing.T) {
	t.Run("Should fail if not found", func(t *testing.T) {
		tb, cleanup := newTestTable(t)
		defer cleanup()

		_, err := tb.Replace(tree.NewKey(types.NewIntegerValue(10)), newObject())
		require.True(t, errs.IsNotFoundError(err))
	})

	t.Run("Should replace the right object", func(t *testing.T) {
		tb, cleanup := newTestTable(t)
		defer cleanup()

		// create two different objects
		doc1 := newObject()
		doc2 := object.NewFieldBuffer().
			Add("fielda", types.NewTextValue("c")).
			Add("fieldb", types.NewTextValue("d"))

		key1, _, err := tb.Insert(doc1)
		assert.NoError(t, err)
		key2, _, err := tb.Insert(doc2)
		assert.NoError(t, err)

		// create a third object
		doc3 := object.NewFieldBuffer().
			Add("fielda", types.NewTextValue("e")).
			Add("fieldb", types.NewTextValue("f"))

		// replace doc1 with doc3
		d3, err := tb.Replace(key1, doc3)
		assert.NoError(t, err)

		// make sure it replaced it correctly
		res, err := tb.GetRow(key1)
		assert.NoError(t, err)
		f, err := res.Get("fielda")
		assert.NoError(t, err)
		require.Equal(t, "e", f.V().(string))

		testutil.RequireObjEqual(t, d3.Object(), res.Object())

		// make sure it didn't also replace the other one
		res, err = tb.GetRow(key2)
		assert.NoError(t, err)
		f, err = res.Get("fielda")
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

		// create two objects
		doc1 := newObject()
		doc2 := newObject()

		_, _, err := tb.Insert(doc1)
		assert.NoError(t, err)
		_, _, err = tb.Insert(doc2)
		assert.NoError(t, err)

		err = tb.Truncate()
		assert.NoError(t, err)

		err = tb.IterateOnRange(nil, false, func(key *tree.Key, _ database.Row) error {
			return errors.New("should not iterate")
		})

		assert.NoError(t, err)
	})
}

// BenchmarkTableInsert benchmarks the Insert method with 1, 10, 1000 and 10000 successive insertions.
func BenchmarkTableInsert(b *testing.B) {
	for size := 1; size <= 10000; size *= 10 {
		b.Run(fmt.Sprintf("%.05d", size), func(b *testing.B) {
			var fb object.FieldBuffer

			for i := int64(0); i < 10; i++ {
				fb.Add(fmt.Sprintf("name-%d", i), types.NewIntegerValue(i))
			}

			b.ResetTimer()
			b.StopTimer()
			for i := 0; i < b.N; i++ {
				tb, cleanup := newTestTable(b)

				b.StartTimer()
				for j := 0; j < size; j++ {
					_, _, _ = tb.Insert(&fb)
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

			var fb object.FieldBuffer

			for i := int64(0); i < 10; i++ {
				fb.Add(fmt.Sprintf("name-%d", i), types.NewIntegerValue(i))
			}

			for i := 0; i < size; i++ {
				_, _, err := tb.Insert(&fb)
				assert.NoError(b, err)
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = tb.IterateOnRange(nil, false, func(*tree.Key, database.Row) error {
					return nil
				})
			}
			b.StopTimer()
		})
	}
}
