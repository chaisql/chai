package database_test

import (
	"fmt"
	"testing"

	"github.com/chaisql/chai/internal/database"
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

	conn, err := db.Connect()
	require.NoError(t, err)
	defer conn.Close()

	tx, err := conn.BeginTx(&database.TxOptions{
		ReadOnly: false,
	})
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
		PrimaryKey: &database.PrimaryKey{
			Columns: []string{"a"},
			Types:   []types.Type{types.TypeText},
		},
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
		TableConstraints: database.TableConstraints{
			&database.TableConstraint{
				PrimaryKey: true,
				Columns:    []string{"a"},
			},
		},
	}
	return createTable(t, tx, ti), fn
}

func createTable(t testing.TB, tx *database.Transaction, info database.TableInfo) *database.Table {
	t.Helper()

	stmt := statement.CreateTableStmt{Info: info}

	res, err := stmt.Run(&statement.Context{
		Conn: tx.Connection(),
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
		Conn: tx.Connection(),
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
		err := row2.Set("a", types.NewTextValue("c"))
		require.NoError(t, err)

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

// TestTableDelete verifies Delete behaviour.
func TestTableDelete(t *testing.T) {
	t.Run("Should not fail if not found", func(t *testing.T) {
		tb, cleanup := newTestTable(t)
		defer cleanup()
		err := tb.Delete(tree.NewKey(types.NewIntegerValue(10)))
		require.NoError(t, err)
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
