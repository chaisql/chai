package database_test

import (
	"testing"

	"github.com/genjidb/genji/database"
	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/engine/memoryengine"
	"github.com/genjidb/genji/index"
	"github.com/stretchr/testify/require"
)

func newTestDB(t testing.TB) (*database.Transaction, func()) {
	db, err := database.New(memoryengine.NewEngine())
	require.NoError(t, err)

	tx, err := db.Begin(true)
	require.NoError(t, err)

	return tx, func() {
		tx.Rollback()
	}
}

// TestTxTable tests all basic operations on tables:
// - CreateTable
// - GetTable
// - DropTable
// - ListTables
func TestTxTable(t *testing.T) {
	t.Run("Create", func(t *testing.T) {
		tx, cleanup := newTestDB(t)
		defer cleanup()

		err := tx.CreateTable("test", nil)
		require.NoError(t, err)

		// Creating a table that already exists should fail.
		err = tx.CreateTable("test", nil)
		require.EqualError(t, err, database.ErrTableAlreadyExists.Error())
	})

	t.Run("Get", func(t *testing.T) {
		tx, cleanup := newTestDB(t)
		defer cleanup()

		err := tx.CreateTable("test", nil)
		require.NoError(t, err)

		table, err := tx.GetTable("test")
		require.NoError(t, err)
		require.Equal(t, "test", table.Name())

		// Getting a table that doesn't exist should fail.
		_, err = tx.GetTable("unknown")
		require.EqualError(t, err, database.ErrTableNotFound.Error())
	})

	t.Run("Drop", func(t *testing.T) {
		tx, cleanup := newTestDB(t)
		defer cleanup()

		err := tx.CreateTable("test", nil)
		require.NoError(t, err)

		err = tx.DropTable("test")
		require.NoError(t, err)

		// Getting a table that has been dropped should fail.
		_, err = tx.GetTable("test")
		require.EqualError(t, err, database.ErrTableNotFound.Error())

		// Dropping a table that doesn't exist should fail.
		err = tx.DropTable("test")
		require.EqualError(t, err, database.ErrTableNotFound.Error())
	})

	t.Run("List", func(t *testing.T) {
		tx, cleanup := newTestDB(t)
		defer cleanup()

		tables, err := tx.ListTables()
		require.NoError(t, err)
		require.Len(t, tables, 0)

		err = tx.CreateTable("foo", nil)
		require.NoError(t, err)

		err = tx.CreateTable("bar", nil)
		require.NoError(t, err)

		err = tx.CreateTable("baz", nil)
		require.NoError(t, err)

		tables, err = tx.ListTables()
		require.NoError(t, err)

		// The returned slice should be lexicographically ordered.
		exp := []string{"bar", "baz", "foo"}
		require.Equal(t, exp, tables)
	})
}

func TestTxCreateIndex(t *testing.T) {
	t.Run("Should create an index and return it", func(t *testing.T) {
		tx, cleanup := newTestDB(t)
		defer cleanup()

		err := tx.CreateTable("test", nil)
		require.NoError(t, err)

		err = tx.CreateIndex(database.IndexConfig{
			IndexName: "idxFoo", TableName: "test", Path: document.NewValuePath("foo"),
		})
		require.NoError(t, err)
		idx, err := tx.GetIndex("idxFoo")
		require.NoError(t, err)
		require.NotNil(t, idx)
	})

	t.Run("Should fail if it already exists", func(t *testing.T) {
		tx, cleanup := newTestDB(t)
		defer cleanup()

		err := tx.CreateTable("test", nil)
		require.NoError(t, err)

		err = tx.CreateIndex(database.IndexConfig{
			IndexName: "idxFoo", TableName: "test", Path: document.NewValuePath("foo"),
		})
		require.NoError(t, err)

		err = tx.CreateIndex(database.IndexConfig{
			IndexName: "idxFoo", TableName: "test", Path: document.NewValuePath("foo"),
		})
		require.Equal(t, database.ErrIndexAlreadyExists, err)
	})

	t.Run("Should fail if table doesn't exists", func(t *testing.T) {
		tx, cleanup := newTestDB(t)
		defer cleanup()

		err := tx.CreateIndex(database.IndexConfig{
			IndexName: "idxFoo", TableName: "test", Path: document.NewValuePath("foo"),
		})
		require.Equal(t, database.ErrTableNotFound, err)
	})
}

func TestTxDropIndex(t *testing.T) {
	t.Run("Should drop an index", func(t *testing.T) {
		tx, cleanup := newTestDB(t)
		defer cleanup()

		err := tx.CreateTable("test", nil)
		require.NoError(t, err)

		err = tx.CreateIndex(database.IndexConfig{
			IndexName: "idxFoo", TableName: "test", Path: document.NewValuePath("foo"),
		})
		require.NoError(t, err)

		err = tx.DropIndex("idxFoo")
		require.NoError(t, err)

		_, err = tx.GetIndex("idxFoo")
		require.Error(t, err)
	})

	t.Run("Should fail if it doesn't exist", func(t *testing.T) {
		tx, cleanup := newTestDB(t)
		defer cleanup()

		err := tx.DropIndex("idxFoo")
		require.Equal(t, database.ErrIndexNotFound, err)
	})
}

func TestTxReIndex(t *testing.T) {
	newTestTableFn := func(t *testing.T) (*database.Transaction, *database.Table, func()) {
		tx, cleanup := newTestDB(t)
		err := tx.CreateTable("test", nil)
		require.NoError(t, err)
		tb, err := tx.GetTable("test")
		require.NoError(t, err)

		for i := int64(0); i < 10; i++ {
			_, err = tb.Insert(document.NewFieldBuffer().
				Add("a", document.NewIntegerValue(i)).
				Add("b", document.NewIntegerValue(i*10)),
			)
			require.NoError(t, err)
		}

		err = tx.CreateIndex(database.IndexConfig{
			IndexName: "a",
			TableName: "test",
			Path:      document.NewValuePath("a"),
		})
		require.NoError(t, err)
		err = tx.CreateIndex(database.IndexConfig{
			IndexName: "b",
			TableName: "test",
			Path:      document.NewValuePath("b"),
		})
		require.NoError(t, err)

		return tx, tb, cleanup
	}

	t.Run("Should fail if not found", func(t *testing.T) {
		tx, _, cleanup := newTestTableFn(t)
		defer cleanup()

		err := tx.ReIndex("foo")
		require.Equal(t, database.ErrIndexNotFound, err)
	})

	t.Run("Should reindex the right index", func(t *testing.T) {
		tx, _, cleanup := newTestTableFn(t)
		defer cleanup()

		err := tx.ReIndex("a")
		require.NoError(t, err)

		idx, err := tx.GetIndex("a")
		require.NoError(t, err)

		var i int
		err = idx.AscendGreaterOrEqual(index.EmptyPivot(document.IntegerValue), func(val document.Value, key []byte) error {
			require.Equal(t, document.NewDoubleValue(float64(i)), val)
			i++
			return nil
		})
		require.Equal(t, 10, i)
		require.NoError(t, err)

		idx, err = tx.GetIndex("b")
		require.NoError(t, err)

		i = 0
		err = idx.AscendGreaterOrEqual(index.EmptyPivot(document.IntegerValue), func(val document.Value, key []byte) error {
			i++
			return nil
		})
		require.NoError(t, err)
		require.Zero(t, i)
	})
}

func TestReIndexAll(t *testing.T) {
	t.Run("Should succeed if not indexes", func(t *testing.T) {
		tx, cleanup := newTestDB(t)
		defer cleanup()

		err := tx.ReIndexAll()
		require.NoError(t, err)
	})

	t.Run("Should reindex all indexes", func(t *testing.T) {
		tx, cleanup := newTestDB(t)
		defer cleanup()

		err := tx.CreateTable("test1", nil)
		require.NoError(t, err)
		tb1, err := tx.GetTable("test1")
		require.NoError(t, err)

		err = tx.CreateTable("test2", nil)
		require.NoError(t, err)
		tb2, err := tx.GetTable("test2")
		require.NoError(t, err)

		for i := int64(0); i < 10; i++ {
			_, err = tb1.Insert(document.NewFieldBuffer().
				Add("a", document.NewIntegerValue(i)).
				Add("b", document.NewIntegerValue(i*10)),
			)
			require.NoError(t, err)
			_, err = tb2.Insert(document.NewFieldBuffer().
				Add("a", document.NewIntegerValue(i)).
				Add("b", document.NewIntegerValue(i*10)),
			)
			require.NoError(t, err)
		}

		err = tx.CreateIndex(database.IndexConfig{
			IndexName: "t1a",
			TableName: "test1",
			Path:      document.NewValuePath("a"),
		})
		require.NoError(t, err)
		err = tx.CreateIndex(database.IndexConfig{
			IndexName: "t2a",
			TableName: "test2",
			Path:      document.NewValuePath("a"),
		})
		require.NoError(t, err)

		err = tx.ReIndexAll()
		require.NoError(t, err)

		idx, err := tx.GetIndex("t1a")
		require.NoError(t, err)

		var i int
		err = idx.AscendGreaterOrEqual(index.EmptyPivot(document.IntegerValue), func(val document.Value, key []byte) error {
			require.Equal(t, document.NewDoubleValue(float64(i)), val)
			i++
			return nil
		})
		require.Equal(t, 10, i)
		require.NoError(t, err)

		idx, err = tx.GetIndex("t2a")
		require.NoError(t, err)

		i = 0
		err = idx.AscendGreaterOrEqual(index.EmptyPivot(document.IntegerValue), func(val document.Value, key []byte) error {
			require.Equal(t, document.NewDoubleValue(float64(i)), val)
			i++
			return nil
		})
		require.Equal(t, 10, i)
		require.NoError(t, err)
	})
}
