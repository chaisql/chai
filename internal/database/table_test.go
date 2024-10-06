package database_test

import (
	"fmt"
	"testing"

	"github.com/chaisql/chai/internal/database"
	"github.com/chaisql/chai/internal/database/catalogstore"
	errs "github.com/chaisql/chai/internal/errors"
	"github.com/chaisql/chai/internal/query/statement"
	"github.com/chaisql/chai/internal/row"
	"github.com/chaisql/chai/internal/testutil"
	"github.com/chaisql/chai/internal/tree"
	"github.com/chaisql/chai/internal/types"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

var errDontCommit = errors.New("don't commit please")

func update(t testing.TB, db *database.Database, fn func(tx *database.Transaction) error) {
	t.Helper()

	tx, err := db.Begin(true)
	require.NoError(t, err)
	defer tx.Rollback()

	err = fn(tx)
	if errors.Is(err, errDontCommit) {
		tx.Rollback()
		return
	}
	require.NoError(t, err)

	err = tx.Commit()
	require.NoError(t, err)
}

func newTestTable(t testing.TB) (*database.Table, func()) {
	t.Helper()

	_, tx, fn := testutil.NewTestTx(t)

	ti := database.TableInfo{
		TableName: "test",
		ColumnConstraints: database.MustNewColumnConstraints(
			&database.ColumnConstraint{
				Position: 0,
				Column:   "a",
				Type:     types.TypeText,
			},
			&database.ColumnConstraint{
				Position: 0,
				Column:   "b",
				Type:     types.TypeText,
			},
		),
	}
	return createTable(t, tx, ti), fn
}

func createTable(t testing.TB, tx *database.Transaction, info database.TableInfo) *database.Table {
	stmt := statement.CreateTableStmt{Info: info}

	res, err := stmt.Run(&statement.Context{
		Tx: tx,
	})
	require.NoError(t, err)
	res.Close()

	tb, err := tx.Catalog.GetTable(tx, stmt.Info.TableName)
	require.NoError(t, err)

	return tb
}

func createTableIfNotExists(t testing.TB, tx *database.Transaction, info database.TableInfo) *database.Table {
	t.Helper()

	stmt := statement.CreateTableStmt{Info: info, IfNotExists: true}

	res, err := stmt.Run(&statement.Context{
		Tx: tx,
	})
	require.NoError(t, err)
	res.Close()

	tb, err := tx.Catalog.GetTable(tx, stmt.Info.TableName)
	require.NoError(t, err)

	return tb
}

func newRow() *row.ColumnBuffer {
	return row.NewColumnBuffer().
		Add("a", types.NewTextValue("a")).
		Add("b", types.NewTextValue("b"))
}

// TestTableGetRow verifies GetRow behaviour.
func TestTableGetRow(t *testing.T) {
	t.Run("Should fail if not found", func(t *testing.T) {
		tb, cleanup := newTestTable(t)
		defer cleanup()

		r, err := tb.GetRow(tree.NewKey(types.NewIntegerValue(10)))
		require.True(t, errs.IsNotFoundError(err))
		require.Nil(t, r)
	})

	t.Run("Should return the right row", func(t *testing.T) {
		tb, cleanup := newTestTable(t)
		defer cleanup()

		// create two rows
		row1 := newRow()
		row2 := newRow()
		row2.Set("a", types.NewTextValue("c"))

		key, _, err := tb.Insert(row1)
		require.NoError(t, err)
		_, _, err = tb.Insert(row2)
		require.NoError(t, err)

		// fetch row1 and make sure it returns the right one
		res, err := tb.GetRow(key)
		require.NoError(t, err)
		v, err := res.Get("a")
		require.NoError(t, err)
		require.Equal(t, "a", types.AsString(v))
	})
}

// TestTableInsert verifies Insert behaviour.
func TestTableInsert(t *testing.T) {
	t.Run("Should generate the right rowid on existing databases", func(t *testing.T) {
		path := t.TempDir()
		db1, err := database.Open(path, &database.Options{
			CatalogLoader: catalogstore.LoadCatalog,
		})
		require.NoError(t, err)

		insertRow := func(db *database.Database) (rawKey *tree.Key) {
			t.Helper()

			update(t, db, func(tx *database.Transaction) error {
				t.Helper()

				// create table if not exists
				ti := database.TableInfo{
					TableName: "test",
					ColumnConstraints: database.MustNewColumnConstraints(
						&database.ColumnConstraint{
							Position: 0,
							Column:   "a",
							Type:     types.TypeText,
						},
						&database.ColumnConstraint{
							Position: 0,
							Column:   "b",
							Type:     types.TypeText,
						},
					),
				}

				tb := createTableIfNotExists(t, tx, ti)

				r := newRow()
				key, _, err := tb.Insert(r)
				require.NoError(t, err)
				require.NotEmpty(t, key)
				rawKey = key
				return nil
			})
			return
		}

		key1 := insertRow(db1)

		err = db1.Close()
		require.NoError(t, err)

		// create a new database object
		db2, err := database.Open(path, &database.Options{
			CatalogLoader: catalogstore.LoadCatalog,
		})
		require.NoError(t, err)

		key2 := insertRow(db2)

		vs, err := key1.Decode()
		require.NoError(t, err)
		a := vs[0].V().(int64)

		vs, err = key2.Decode()
		require.NoError(t, err)
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
		require.NoError(t, err)
	})

	t.Run("Should delete the right object", func(t *testing.T) {
		tb, cleanup := newTestTable(t)
		defer cleanup()

		// create two objects, one with an additional field
		row1 := newRow()
		row1.Add("fieldc", types.NewIntegerValue(40))
		row2 := newRow()

		key1, _, err := tb.Insert(testutil.CloneRow(t, row1))
		require.NoError(t, err)
		key2, _, err := tb.Insert(testutil.CloneRow(t, row2))
		require.NoError(t, err)

		// delete the object
		err = tb.Delete(key1)
		require.NoError(t, err)

		// try again, should fail
		_, err = tb.GetRow(key1)
		require.True(t, errs.IsNotFoundError(err))

		// make sure it didn't also delete the other one
		res, err := tb.GetRow(key2)
		require.NoError(t, err)
		_, err = res.Get("fieldc")
		require.Error(t, err)
	})
}

// TestTableReplace verifies Replace behaviour.
func TestTableReplace(t *testing.T) {
	t.Run("Should fail if not found", func(t *testing.T) {
		tb, cleanup := newTestTable(t)
		defer cleanup()

		_, err := tb.Replace(tree.NewKey(types.NewIntegerValue(10)), newRow())
		require.True(t, errs.IsNotFoundError(err))
	})

	t.Run("Should replace the right row", func(t *testing.T) {
		tb, cleanup := newTestTable(t)
		defer cleanup()

		// create two different objects
		row1 := newRow()
		row2 := row.NewColumnBuffer().
			Add("a", types.NewTextValue("c")).
			Add("b", types.NewTextValue("d"))

		key1, _, err := tb.Insert(row1)
		require.NoError(t, err)
		key2, _, err := tb.Insert(row2)
		require.NoError(t, err)

		// create a third object
		doc3 := row.NewColumnBuffer().
			Add("a", types.NewTextValue("e")).
			Add("b", types.NewTextValue("f"))

		// replace row1 with doc3
		d3, err := tb.Replace(key1, doc3)
		require.NoError(t, err)

		// make sure it replaced it correctly
		res, err := tb.GetRow(key1)
		require.NoError(t, err)
		f, err := res.Get("a")
		require.NoError(t, err)
		require.Equal(t, "e", f.V().(string))

		testutil.RequireRowEqual(t, d3, res)

		// make sure it didn't also replace the other one
		res, err = tb.GetRow(key2)
		require.NoError(t, err)
		f, err = res.Get("a")
		require.NoError(t, err)
		require.Equal(t, "c", f.V().(string))
	})
}

// TestTableTruncate verifies Truncate behaviour.
func TestTableTruncate(t *testing.T) {
	t.Run("Should succeed if table empty", func(t *testing.T) {
		tb, cleanup := newTestTable(t)
		defer cleanup()

		err := tb.Truncate()
		require.NoError(t, err)
	})

	t.Run("Should truncate the table", func(t *testing.T) {
		tb, cleanup := newTestTable(t)
		defer cleanup()

		// create two objects
		row1 := newRow()
		row2 := newRow()

		_, _, err := tb.Insert(row1)
		require.NoError(t, err)
		_, _, err = tb.Insert(row2)
		require.NoError(t, err)

		err = tb.Truncate()
		require.NoError(t, err)

		it, err := tb.Iterator(nil)
		require.NoError(t, err)
		defer it.Close()

		it.First()
		require.False(t, it.Valid())
	})
}

// BenchmarkTableInsert benchmarks the Insert method with 1, 10, 1000 and 10000 successive insertions.
func BenchmarkTableInsert(b *testing.B) {
	for size := 1; size <= 10000; size *= 10 {
		b.Run(fmt.Sprintf("%.05d", size), func(b *testing.B) {
			var fb row.ColumnBuffer

			for i := int64(0); i < 10; i++ {
				fb.Add(fmt.Sprintf("name-%d", i), types.NewBigintValue(i))
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

			var fb row.ColumnBuffer

			for i := int64(0); i < 10; i++ {
				fb.Add(fmt.Sprintf("name-%d", i), types.NewBigintValue(i))
			}

			for i := 0; i < size; i++ {
				_, _, err := tb.Insert(&fb)
				require.NoError(b, err)
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				it, err := tb.Iterator(nil)
				require.NoError(b, err)

				for it.First(); it.Valid(); it.Next() {
				}

				it.Close()

				// _ = tb.IterateOnRange(nil, false, func(*tree.Key, database.Row) error {
				// 	return nil
				// })
			}
			b.StopTimer()
		})
	}
}
