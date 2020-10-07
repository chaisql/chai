package database_test

import (
	"context"
	"testing"

	"github.com/genjidb/genji/database"
	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/document/encoding/msgpack"
	"github.com/genjidb/genji/engine/memoryengine"
	"github.com/genjidb/genji/key"
	"github.com/stretchr/testify/require"
)

func newTestDB(t testing.TB) (*database.Transaction, func()) {
	ctx := context.Background()

	db, err := database.New(ctx, memoryengine.NewEngine(), database.Options{Codec: msgpack.NewCodec()})
	require.NoError(t, err)

	tx, err := db.Begin(ctx, true)
	require.NoError(t, err)

	return tx, func() {
		tx.Rollback()
	}
}

// TestTxTable tests all basic operations on tables:
// - CreateTable
// - GetTable
// - DropTable
// - RenameTable
func TestTxTable(t *testing.T) {
	ctx := context.Background()

	t.Run("Create", func(t *testing.T) {
		tx, cleanup := newTestDB(t)
		defer cleanup()

		err := tx.CreateTable(ctx, "test", nil)
		require.NoError(t, err)

		// Creating a table that already exists should fail.
		err = tx.CreateTable(ctx, "test", nil)
		require.EqualError(t, err, database.ErrTableAlreadyExists.Error())

		// Creating a table that starts with __genji_ should fail.
		err = tx.CreateTable(ctx, "__genji_foo", nil)
		require.Error(t, err)
	})

	t.Run("Create and rollback", func(t *testing.T) {
		db, err := database.New(ctx, memoryengine.NewEngine(), database.Options{Codec: msgpack.NewCodec()})
		require.NoError(t, err)
		defer db.Close()

		check := func() {
			tx, err := db.Begin(ctx, true)
			require.NoError(t, err)
			defer func() {
				err = tx.Rollback()
				require.NoError(t, err)
			}()

			err = tx.CreateTable(ctx, "test", nil)
			require.NoError(t, err)
		}

		check()
		check()
	})

	t.Run("Get", func(t *testing.T) {
		tx, cleanup := newTestDB(t)
		defer cleanup()

		err := tx.CreateTable(ctx, "test", nil)
		require.NoError(t, err)

		table, err := tx.GetTable(ctx, "test")
		require.NoError(t, err)
		require.Equal(t, "test", table.Name())

		// Getting a table that doesn't exist should fail.
		_, err = tx.GetTable(ctx, "unknown")
		require.EqualError(t, err, database.ErrTableNotFound.Error())
	})

	t.Run("Drop", func(t *testing.T) {
		tx, cleanup := newTestDB(t)
		defer cleanup()

		err := tx.CreateTable(ctx, "test", nil)
		require.NoError(t, err)

		err = tx.DropTable(ctx, "test")
		require.NoError(t, err)

		// Getting a table that has been dropped should fail.
		_, err = tx.GetTable(ctx, "test")
		require.EqualError(t, err, database.ErrTableNotFound.Error())

		// Dropping a table that doesn't exist should fail.
		err = tx.DropTable(ctx, "test")
		require.EqualError(t, err, database.ErrTableNotFound.Error())
	})

	t.Run("Rename", func(t *testing.T) {
		tx, cleanup := newTestDB(t)
		defer cleanup()

		ti := &database.TableInfo{FieldConstraints: []database.FieldConstraint{
			{Path: parsePath(t, "name"), Type: document.TextValue, IsNotNull: true},
			{Path: parsePath(t, "age"), Type: document.IntegerValue, IsPrimaryKey: true},
			{Path: parsePath(t, "gender"), Type: document.TextValue},
			{Path: parsePath(t, "city"), Type: document.TextValue},
		}}
		err := tx.CreateTable(ctx, "foo", ti)
		require.NoError(t, err)

		err = tx.CreateIndex(ctx, database.IndexConfig{Path: parsePath(t, "gender"), IndexName: "idx_gender", TableName: "foo"})
		require.NoError(t, err)
		err = tx.CreateIndex(ctx, database.IndexConfig{Path: parsePath(t, "city"), IndexName: "idx_city", TableName: "foo", Unique: true})
		require.NoError(t, err)

		err = tx.RenameTable(ctx, "foo", "zoo")
		require.NoError(t, err)

		// Getting the old table should return an error.
		_, err = tx.GetTable(ctx, "foo")
		require.EqualError(t, database.ErrTableNotFound, err.Error())

		tb, err := tx.GetTable(ctx, "zoo")
		require.NoError(t, err)

		// The field constraints should be the same.
		info, err := tb.Info(ctx)
		require.NoError(t, err)
		require.Equal(t, ti.FieldConstraints, info.FieldConstraints)

		// Check that the indexes have been updated as well.
		idxs, err := tx.ListIndexes(ctx)
		require.NoError(t, err)
		require.Len(t, idxs, 2)
		for _, idx := range idxs {
			require.Equal(t, "zoo", idx.TableName)
		}

		// Renaming a non existing table should return an error
		err = tx.RenameTable(ctx, "foo", "")
		require.EqualError(t, database.ErrTableNotFound, err.Error())
	})
}

func TestTxCreateIndex(t *testing.T) {
	ctx := context.Background()

	t.Run("Should create an index and return it", func(t *testing.T) {
		tx, cleanup := newTestDB(t)
		defer cleanup()

		err := tx.CreateTable(ctx, "test", nil)
		require.NoError(t, err)

		err = tx.CreateIndex(ctx, database.IndexConfig{
			IndexName: "idxFoo", TableName: "test", Path: parsePath(t, "foo"),
		})
		require.NoError(t, err)
		idx, err := tx.GetIndex(ctx, "idxFoo")
		require.NoError(t, err)
		require.NotNil(t, idx)
	})

	t.Run("Should fail if it already exists", func(t *testing.T) {
		tx, cleanup := newTestDB(t)
		defer cleanup()

		err := tx.CreateTable(ctx, "test", nil)
		require.NoError(t, err)

		err = tx.CreateIndex(ctx, database.IndexConfig{
			IndexName: "idxFoo", TableName: "test", Path: parsePath(t, "foo"),
		})
		require.NoError(t, err)

		err = tx.CreateIndex(ctx, database.IndexConfig{
			IndexName: "idxFoo", TableName: "test", Path: parsePath(t, "foo"),
		})
		require.Equal(t, database.ErrIndexAlreadyExists, err)
	})

	t.Run("Should fail if table doesn't exists", func(t *testing.T) {
		tx, cleanup := newTestDB(t)
		defer cleanup()

		err := tx.CreateIndex(ctx, database.IndexConfig{
			IndexName: "idxFoo", TableName: "test", Path: parsePath(t, "foo"),
		})
		require.Equal(t, database.ErrTableNotFound, err)
	})
}

func TestTxDropIndex(t *testing.T) {
	ctx := context.Background()

	t.Run("Should drop an index", func(t *testing.T) {
		tx, cleanup := newTestDB(t)
		defer cleanup()

		err := tx.CreateTable(ctx, "test", nil)
		require.NoError(t, err)

		err = tx.CreateIndex(ctx, database.IndexConfig{
			IndexName: "idxFoo", TableName: "test", Path: parsePath(t, "foo"),
		})
		require.NoError(t, err)

		err = tx.DropIndex(ctx, "idxFoo")
		require.NoError(t, err)

		_, err = tx.GetIndex(ctx, "idxFoo")
		require.Error(t, err)
	})

	t.Run("Should fail if it doesn't exist", func(t *testing.T) {
		tx, cleanup := newTestDB(t)
		defer cleanup()

		err := tx.DropIndex(ctx, "idxFoo")
		require.Equal(t, database.ErrIndexNotFound, err)
	})
}

func TestTxReIndex(t *testing.T) {
	ctx := context.Background()

	newTestTableFn := func(t *testing.T) (*database.Transaction, *database.Table, func()) {
		tx, cleanup := newTestDB(t)
		err := tx.CreateTable(ctx, "test", nil)
		require.NoError(t, err)
		tb, err := tx.GetTable(ctx, "test")
		require.NoError(t, err)

		for i := int64(0); i < 10; i++ {
			_, err = tb.Insert(ctx, document.NewFieldBuffer().
				Add("a", document.NewIntegerValue(i)).
				Add("b", document.NewIntegerValue(i*10)),
			)
			require.NoError(t, err)
		}

		err = tx.CreateIndex(ctx, database.IndexConfig{
			IndexName: "a",
			TableName: "test",
			Path:      parsePath(t, "a"),
		})
		require.NoError(t, err)
		err = tx.CreateIndex(ctx, database.IndexConfig{
			IndexName: "b",
			TableName: "test",
			Path:      parsePath(t, "b"),
		})
		require.NoError(t, err)

		return tx, tb, cleanup
	}

	t.Run("Should fail if not found", func(t *testing.T) {
		tx, _, cleanup := newTestTableFn(t)
		defer cleanup()

		err := tx.ReIndex(ctx, "foo")
		require.Equal(t, database.ErrIndexNotFound, err)
	})

	t.Run("Should not fail if field not found", func(t *testing.T) {
		tx, cleanup := newTestDB(t)
		defer cleanup()

		err := tx.CreateTable(ctx, "test", nil)
		require.NoError(t, err)
		tb, err := tx.GetTable(ctx, "test")
		require.NoError(t, err)

		_, err = tb.Insert(ctx, document.NewFieldBuffer().
			Add("a", document.NewIntegerValue(1)),
		)
		require.NoError(t, err)

		err = tx.CreateIndex(ctx, database.IndexConfig{
			IndexName: "b",
			TableName: "test",
			Path:      parsePath(t, "b"),
		})

		err = tx.ReIndex(ctx, "b")
		require.NoError(t, err)
	})

	t.Run("Should reindex the right index", func(t *testing.T) {
		tx, _, cleanup := newTestTableFn(t)
		defer cleanup()

		err := tx.ReIndex(ctx, "a")
		require.NoError(t, err)

		idx, err := tx.GetIndex(ctx, "a")
		require.NoError(t, err)

		var i int
		err = idx.AscendGreaterOrEqual(ctx, document.Value{Type: document.IntegerValue}, func(v, k []byte, isEqual bool) error {
			enc, err := key.AppendValue(nil, document.NewIntegerValue(int64(i)))
			require.NoError(t, err)
			require.Equal(t, enc, v)
			i++
			return nil
		})
		require.Equal(t, 10, i)
		require.NoError(t, err)

		idx, err = tx.GetIndex(ctx, "b")
		require.NoError(t, err)

		i = 0
		err = idx.AscendGreaterOrEqual(ctx, document.Value{Type: document.IntegerValue}, func(val, key []byte, isEqual bool) error {
			i++
			return nil
		})
		require.NoError(t, err)
		require.Zero(t, i)
	})
}

func TestReIndexAll(t *testing.T) {
	ctx := context.Background()

	t.Run("Should succeed if not indexes", func(t *testing.T) {
		tx, cleanup := newTestDB(t)
		defer cleanup()

		err := tx.ReIndexAll(ctx)
		require.NoError(t, err)
	})

	t.Run("Should reindex all indexes", func(t *testing.T) {
		tx, cleanup := newTestDB(t)
		defer cleanup()

		err := tx.CreateTable(ctx, "test1", nil)
		require.NoError(t, err)
		tb1, err := tx.GetTable(ctx, "test1")
		require.NoError(t, err)

		err = tx.CreateTable(ctx, "test2", nil)
		require.NoError(t, err)
		tb2, err := tx.GetTable(ctx, "test2")
		require.NoError(t, err)

		for i := int64(0); i < 10; i++ {
			_, err = tb1.Insert(ctx, document.NewFieldBuffer().
				Add("a", document.NewIntegerValue(i)).
				Add("b", document.NewIntegerValue(i*10)),
			)
			require.NoError(t, err)
			_, err = tb2.Insert(ctx, document.NewFieldBuffer().
				Add("a", document.NewIntegerValue(i)).
				Add("b", document.NewIntegerValue(i*10)),
			)
			require.NoError(t, err)
		}

		err = tx.CreateIndex(ctx, database.IndexConfig{
			IndexName: "t1a",
			TableName: "test1",
			Path:      parsePath(t, "a"),
		})
		require.NoError(t, err)
		err = tx.CreateIndex(ctx, database.IndexConfig{
			IndexName: "t2a",
			TableName: "test2",
			Path:      parsePath(t, "a"),
		})
		require.NoError(t, err)

		err = tx.ReIndexAll(ctx)
		require.NoError(t, err)

		idx, err := tx.GetIndex(ctx, "t1a")
		require.NoError(t, err)

		var i int
		err = idx.AscendGreaterOrEqual(ctx, document.Value{Type: document.IntegerValue}, func(v, k []byte, isEqual bool) error {
			enc, err := key.AppendValue(nil, document.NewIntegerValue(int64(i)))
			require.NoError(t, err)
			require.Equal(t, enc, v)
			i++
			return nil
		})
		require.Equal(t, 10, i)
		require.NoError(t, err)

		idx, err = tx.GetIndex(ctx, "t2a")
		require.NoError(t, err)

		i = 0
		err = idx.AscendGreaterOrEqual(ctx, document.Value{Type: document.IntegerValue}, func(v, k []byte, isEqual bool) error {
			enc, err := key.AppendValue(nil, document.NewIntegerValue(int64(i)))
			require.NoError(t, err)
			require.Equal(t, enc, v)
			i++
			return nil
		})
		require.Equal(t, 10, i)
		require.NoError(t, err)
	})
}
