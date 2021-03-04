package database_test

import (
	"bytes"
	"errors"
	"testing"

	"github.com/genjidb/genji"
	"github.com/genjidb/genji/database"
	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/testutil"
	"github.com/stretchr/testify/require"
)

var errDontCommit = errors.New("don't commit please")

func update(t testing.TB, db *database.Database, fn func(tx *database.Transaction) error) {
	t.Helper()

	tx, err := db.Begin(true)
	require.NoError(t, err)
	defer tx.Rollback()

	err = fn(tx)
	if err == errDontCommit {
		tx.Rollback()
		return
	}
	require.NoError(t, err)

	err = tx.Commit()
	require.NoError(t, err)
}

// TestCatalogTable tests all basic operations on tables:
// - CreateTable
// - GetTable
// - DropTable
// - RenameTable
// - AddFieldConstraint
func TestCatalogTable(t *testing.T) {
	t.Run("Get", func(t *testing.T) {
		db, cleanup := newTestDB(t)
		defer cleanup()

		catalog := db.Catalog()

		update(t, db, func(tx *database.Transaction) error {
			return catalog.CreateTable(tx, "test", nil)
		})

		update(t, db, func(tx *database.Transaction) error {
			table, err := tx.GetTable("test")
			require.NoError(t, err)
			require.Equal(t, "test", table.Name())

			// Getting a table that doesn't exist should fail.
			_, err = tx.GetTable("unknown")
			if !errors.Is(err, database.ErrTableNotFound) {
				require.Equal(t, err, database.ErrTableNotFound)
			}

			return nil
		})
	})

	t.Run("Drop", func(t *testing.T) {
		db, cleanup := newTestDB(t)
		defer cleanup()

		catalog := db.Catalog()

		update(t, db, func(tx *database.Transaction) error {
			return catalog.CreateTable(tx, "test", nil)
		})

		clone := catalog.Clone()

		update(t, db, func(tx *database.Transaction) error {
			err := tx.DropTable("test")
			require.NoError(t, err)

			// Getting a table that has been dropped should fail.
			_, err = tx.GetTable("test")
			if !errors.Is(err, database.ErrTableNotFound) {
				require.Equal(t, err, database.ErrTableNotFound)
			}

			// Dropping a table that doesn't exist should fail.
			err = tx.DropTable("test")
			if !errors.Is(err, database.ErrTableNotFound) {
				require.Equal(t, err, database.ErrTableNotFound)
			}

			return errDontCommit
		})

		require.Equal(t, clone, catalog)
	})

	t.Run("Rename", func(t *testing.T) {
		db, cleanup := newTestDB(t)
		defer cleanup()

		catalog := db.Catalog()

		ti := &database.TableInfo{FieldConstraints: []*database.FieldConstraint{
			{Path: parsePath(t, "name"), Type: document.TextValue, IsNotNull: true},
			{Path: parsePath(t, "age"), Type: document.IntegerValue, IsPrimaryKey: true},
			{Path: parsePath(t, "gender"), Type: document.TextValue},
			{Path: parsePath(t, "city"), Type: document.TextValue},
		}}

		update(t, db, func(tx *database.Transaction) error {
			err := catalog.CreateTable(tx, "foo", ti)
			require.NoError(t, err)

			err = catalog.CreateIndex(tx, &database.IndexInfo{Paths: []document.Path{parsePath(t, "gender")}, IndexName: "idx_gender", TableName: "foo"})
			require.NoError(t, err)
			err = catalog.CreateIndex(tx, &database.IndexInfo{Paths: []document.Path{parsePath(t, "city")}, IndexName: "idx_city", TableName: "foo", Unique: true})
			require.NoError(t, err)

			return nil
		})

		clone := catalog.Clone()

		update(t, db, func(tx *database.Transaction) error {
			err := tx.RenameTable("foo", "zoo")
			require.NoError(t, err)

			// Getting the old table should return an error.
			_, err = tx.GetTable("foo")
			if !errors.Is(err, database.ErrTableNotFound) {
				require.Equal(t, err, database.ErrTableNotFound)
			}

			tb, err := tx.GetTable("zoo")
			require.NoError(t, err)
			// The field constraints should be the same.
			info := tb.Info()
			require.Equal(t, ti.FieldConstraints, info.FieldConstraints)

			// Check that the indexes have been updated as well.
			idxs := tx.ListIndexes()
			require.Len(t, idxs, 2)
			for _, name := range idxs {
				idx, err := tx.GetIndex(name)
				require.NoError(t, err)
				require.Equal(t, "zoo", idx.Info.TableName)
			}

			// Renaming a non existing table should return an error
			err = tx.RenameTable("foo", "")
			if !errors.Is(err, database.ErrTableNotFound) {
				require.Equal(t, err, database.ErrTableNotFound)
			}

			return errDontCommit
		})

		require.Equal(t, clone, catalog)
	})

	t.Run("Add field constraint", func(t *testing.T) {
		db, cleanup := newTestDB(t)
		defer cleanup()

		catalog := db.Catalog()

		ti := &database.TableInfo{FieldConstraints: []*database.FieldConstraint{
			{Path: parsePath(t, "name"), Type: document.TextValue, IsNotNull: true},
			{Path: parsePath(t, "age"), Type: document.IntegerValue, IsPrimaryKey: true},
			{Path: parsePath(t, "gender"), Type: document.TextValue},
			{Path: parsePath(t, "city"), Type: document.TextValue},
		}}

		update(t, db, func(tx *database.Transaction) error {
			return catalog.CreateTable(tx, "foo", ti)
		})

		clone := catalog.Clone()

		update(t, db, func(tx *database.Transaction) error {

			// Add field constraint
			fieldToAdd := database.FieldConstraint{
				Path: parsePath(t, "last_name"), Type: document.TextValue,
			}
			err := tx.AddFieldConstraint("foo", fieldToAdd)
			require.NoError(t, err)

			tb, err := tx.GetTable("foo")
			require.NoError(t, err)

			// The field constraints should not be the same.
			info := tb.Info()
			require.Contains(t, info.FieldConstraints, &fieldToAdd)

			// Renaming a non existing table should return an error
			err = tx.AddFieldConstraint("bar", fieldToAdd)
			if !errors.Is(err, database.ErrTableNotFound) {
				require.Equal(t, err, database.ErrTableNotFound)
			}

			// Adding a existing field should return an error
			err = tx.AddFieldConstraint("foo", *ti.FieldConstraints[0])
			require.Error(t, err)

			// Adding a second primary key should return an error
			fieldToAdd = database.FieldConstraint{
				Path: parsePath(t, "foobar"), Type: document.IntegerValue, IsPrimaryKey: true,
			}
			err = tx.AddFieldConstraint("foo", fieldToAdd)
			require.Error(t, err)

			return errDontCommit
		})

		require.Equal(t, clone, catalog)
	})
}

func TestCatalogCreate(t *testing.T) {
	t.Run("Same table name", func(t *testing.T) {
		db, cleanup := newTestDB(t)
		defer cleanup()

		catalog := db.Catalog()

		clone := catalog.Clone()

		update(t, db, func(tx *database.Transaction) error {
			err := catalog.CreateTable(tx, "test", nil)
			require.NoError(t, err)

			// Creating a table that already exists should fail.
			err = catalog.CreateTable(tx, "test", nil)
			require.EqualError(t, err, database.ErrTableAlreadyExists.Error())

			// Creating a table that starts with __genji_ should fail.
			err = catalog.CreateTable(tx, "__genji_foo", nil)
			require.Error(t, err)

			return errDontCommit
		})

		require.Equal(t, clone, catalog)
	})

	t.Run("Create and rollback", func(t *testing.T) {
		db, cleanup := newTestDB(t)
		defer cleanup()

		check := func() {
			update(t, db, func(tx *database.Transaction) error {
				err := db.Catalog().CreateTable(tx, "test", nil)
				require.NoError(t, err)

				return errDontCommit
			})
		}

		check()
		check()
	})

	t.Run("Invalid constraints", func(t *testing.T) {
		db, cleanup := newTestDB(t)
		defer cleanup()

		catalog := db.Catalog()

		clone := catalog.Clone()

		update(t, db, func(tx *database.Transaction) error {
			err := catalog.CreateTable(tx, "test", &database.TableInfo{
				FieldConstraints: []*database.FieldConstraint{
					{Path: document.NewPath("a", "b"), Type: document.IntegerValue},
					{Path: document.NewPath("a"), Type: document.IntegerValue},
				},
			})
			require.Error(t, err)
			return errDontCommit
		})

		require.Equal(t, clone, catalog)
	})
}

func TestTxCreateIndex(t *testing.T) {
	t.Run("Should create an index, build it and return it", func(t *testing.T) {
		db, cleanup := newTestDB(t)
		defer cleanup()
		catalog := db.Catalog()

		update(t, db, func(tx *database.Transaction) error {
			err := catalog.CreateTable(tx, "test", nil)
			if err != nil {
				return err
			}

			tb, err := tx.GetTable("test")
			require.NoError(t, err)

			for i := int64(0); i < 10; i++ {
				_, err = tb.Insert(document.NewFieldBuffer().
					Add("a", document.NewIntegerValue(i)).
					Add("b", document.NewIntegerValue(i*10)),
				)
				require.NoError(t, err)
			}

			return nil
		})

		clone := catalog.Clone()

		update(t, db, func(tx *database.Transaction) error {
			err := catalog.CreateIndex(tx, &database.IndexInfo{
				IndexName: "idx_a", TableName: "test", Paths: []document.Path{parsePath(t, "a")},
			})
			require.NoError(t, err)
			idx, err := tx.GetIndex("idx_a")
			require.NoError(t, err)
			require.NotNil(t, idx)

			var i int
			err = idx.AscendGreaterOrEqual(document.Value{Type: document.DoubleValue}, func(v, k []byte) error {
				var buf bytes.Buffer
				err = document.NewValueEncoder(&buf).Encode(document.NewDoubleValue(float64(i)))
				require.NoError(t, err)
				enc := buf.Bytes()
				require.Equal(t, enc, v)
				i++
				return nil
			})
			require.Equal(t, 10, i)
			require.NoError(t, err)

			return errDontCommit
		})

		require.Equal(t, clone, catalog)
	})

	t.Run("Should fail if it already exists", func(t *testing.T) {
		db, cleanup := newTestDB(t)
		defer cleanup()

		catalog := db.Catalog()
		update(t, db, func(tx *database.Transaction) error {
			return catalog.CreateTable(tx, "test", nil)
		})

		update(t, db, func(tx *database.Transaction) error {
			err := catalog.CreateIndex(tx, &database.IndexInfo{
				IndexName: "idxFoo", TableName: "test", Paths: []document.Path{parsePath(t, "foo")},
			})
			require.NoError(t, err)

			err = catalog.CreateIndex(tx, &database.IndexInfo{
				IndexName: "idxFoo", TableName: "test", Paths: []document.Path{parsePath(t, "foo")},
			})
			require.Equal(t, database.ErrIndexAlreadyExists, err)
			return nil
		})
	})

	t.Run("Should fail if table doesn't exist", func(t *testing.T) {
		db, cleanup := newTestDB(t)
		defer cleanup()
		catalog := db.Catalog()
		update(t, db, func(tx *database.Transaction) error {
			err := catalog.CreateIndex(tx, &database.IndexInfo{
				IndexName: "idxFoo", TableName: "test", Paths: []document.Path{parsePath(t, "foo")},
			})
			if !errors.Is(err, database.ErrTableNotFound) {
				require.Equal(t, err, database.ErrTableNotFound)
			}

			return nil
		})
	})

	t.Run("Should generate a name if not provided", func(t *testing.T) {
		db, cleanup := newTestDB(t)
		defer cleanup()
		catalog := db.Catalog()

		update(t, db, func(tx *database.Transaction) error {
			return catalog.CreateTable(tx, "test", nil)
		})

		update(t, db, func(tx *database.Transaction) error {
			err := catalog.CreateIndex(tx, &database.IndexInfo{
				TableName: "test", Paths: []document.Path{parsePath(t, "foo")},
			})
			require.NoError(t, err)

			_, err = catalog.GetIndex(tx, "__genji_autoindex_test_1")
			require.NoError(t, err)

			// create another one
			err = catalog.CreateIndex(tx, &database.IndexInfo{
				TableName: "test", Paths: []document.Path{parsePath(t, "foo")},
			})
			require.NoError(t, err)

			_, err = catalog.GetIndex(tx, "__genji_autoindex_test_2")
			require.NoError(t, err)
			return nil
		})
	})
}

func TestTxDropIndex(t *testing.T) {
	t.Run("Should drop an index", func(t *testing.T) {
		db, cleanup := newTestDB(t)
		defer cleanup()
		catalog := db.Catalog()

		update(t, db, func(tx *database.Transaction) error {
			err := catalog.CreateTable(tx, "test", nil)
			require.NoError(t, err)
			err = catalog.CreateIndex(tx, &database.IndexInfo{
				IndexName: "idxFoo", TableName: "test", Paths: []document.Path{parsePath(t, "foo")},
			})
			require.NoError(t, err)
			err = catalog.CreateIndex(tx, &database.IndexInfo{
				IndexName: "idxBar", TableName: "test", Paths: []document.Path{parsePath(t, "bar")},
			})
			require.NoError(t, err)
			return nil
		})

		clone := catalog.Clone()
		update(t, db, func(tx *database.Transaction) error {
			err := catalog.DropIndex(tx, "idxFoo")
			require.NoError(t, err)

			_, err = tx.GetIndex("idxFoo")
			require.Error(t, err)

			_, err = tx.GetIndex("idxBar")
			require.NoError(t, err)

			// cf: https://github.com/genjidb/genji/issues/360
			_, err = tx.GetTable("test")
			require.NoError(t, err)

			return errDontCommit
		})

		require.Equal(t, clone, catalog)
	})

	t.Run("Should fail if it doesn't exist", func(t *testing.T) {
		db, cleanup := newTestDB(t)
		defer cleanup()
		catalog := db.Catalog()

		update(t, db, func(tx *database.Transaction) error {
			err := catalog.DropIndex(tx, "idxFoo")
			require.Equal(t, database.ErrIndexNotFound, err)
			return nil
		})
	})
}

func TestCatalogReIndex(t *testing.T) {
	prepareTableFn := func(t *testing.T, db *database.Database) {
		catalog := db.Catalog()

		update(t, db, func(tx *database.Transaction) error {
			err := catalog.CreateTable(tx, "test", nil)
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

			err = catalog.CreateIndex(tx, &database.IndexInfo{
				IndexName: "a",
				TableName: "test",
				Paths:     []document.Path{parsePath(t, "a")},
			})
			require.NoError(t, err)
			err = catalog.CreateIndex(tx, &database.IndexInfo{
				IndexName: "b",
				TableName: "test",
				Paths:     []document.Path{parsePath(t, "b")},
			})
			require.NoError(t, err)

			return nil
		})
	}

	t.Run("Should fail if not found", func(t *testing.T) {
		db, cleanup := newTestDB(t)
		defer cleanup()

		prepareTableFn(t, db)

		update(t, db, func(tx *database.Transaction) error {
			err := tx.ReIndex("foo")
			require.Equal(t, database.ErrIndexNotFound, err)
			return nil
		})
	})

	t.Run("Should not fail if field not found", func(t *testing.T) {
		db, cleanup := newTestDB(t)
		defer cleanup()

		catalog := db.Catalog()

		update(t, db, func(tx *database.Transaction) error {
			err := catalog.CreateTable(tx, "test", nil)
			require.NoError(t, err)

			tb, err := tx.GetTable("test")
			require.NoError(t, err)

			_, err = tb.Insert(document.NewFieldBuffer().
				Add("a", document.NewIntegerValue(1)),
			)
			require.NoError(t, err)

			return catalog.CreateIndex(tx, &database.IndexInfo{
				IndexName: "b",
				TableName: "test",
				Paths:     []document.Path{parsePath(t, "b")},
			})
		})

		clone := catalog.Clone()

		update(t, db, func(tx *database.Transaction) error {
			err := catalog.ReIndex(tx, "b")
			require.NoError(t, err)

			return errDontCommit
		})

		require.Equal(t, clone, catalog)
	})

	t.Run("Should reindex the index", func(t *testing.T) {
		db, cleanup := newTestDB(t)
		defer cleanup()

		prepareTableFn(t, db)

		catalog := db.Catalog()
		clone := catalog.Clone()

		update(t, db, func(tx *database.Transaction) error {
			err := tx.ReIndex("a")
			require.NoError(t, err)

			idx, err := tx.GetIndex("a")
			require.NoError(t, err)

			var i int
			err = idx.AscendGreaterOrEqual([]document.Value{document.Value{Type: document.DoubleValue}}, func(v, k []byte) error {
				var buf bytes.Buffer
				err = document.NewValueEncoder(&buf).Encode(document.NewDoubleValue(float64(i)))
				require.NoError(t, err)
				enc := buf.Bytes()
				require.Equal(t, enc, v)
				i++
				return nil
			})
			require.Equal(t, 10, i)
			require.NoError(t, err)

			_, err = tx.GetIndex("b")
			require.NoError(t, err)

			return errDontCommit
		})

		require.Equal(t, clone, catalog)
	})
}

func TestReIndexAll(t *testing.T) {
	t.Run("Should succeed if no indexes", func(t *testing.T) {
		db, cleanup := newTestDB(t)
		defer cleanup()

		update(t, db, func(tx *database.Transaction) error {
			catalog := db.Catalog()
			err := catalog.ReIndexAll(tx)
			require.NoError(t, err)
			return nil
		})
	})

	t.Run("Should reindex all indexes", func(t *testing.T) {
		db, cleanup := newTestDB(t)
		defer cleanup()

		catalog := db.Catalog()

		update(t, db, func(tx *database.Transaction) error {
			err := catalog.CreateTable(tx, "test1", nil)
			require.NoError(t, err)
			tb1, err := tx.GetTable("test1")
			require.NoError(t, err)

			err = catalog.CreateTable(tx, "test2", nil)
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

			err = catalog.CreateIndex(tx, &database.IndexInfo{
				IndexName: "t1a",
				TableName: "test1",
				Paths:     []document.Path{parsePath(t, "a")},
			})
			require.NoError(t, err)
			err = catalog.CreateIndex(tx, &database.IndexInfo{
				IndexName: "t2a",
				TableName: "test2",
				Paths:     []document.Path{parsePath(t, "a")},
			})
			require.NoError(t, err)

			return nil
		})

		clone := catalog.Clone()

		update(t, db, func(tx *database.Transaction) error {
			err := tx.ReIndexAll()
			require.NoError(t, err)
			idx, err := tx.GetIndex("t1a")
			require.NoError(t, err)

			var i int
			err = idx.AscendGreaterOrEqual([]document.Value{document.Value{Type: document.DoubleValue}}, func(v, k []byte) error {
				var buf bytes.Buffer
				err = document.NewValueEncoder(&buf).Encode(document.NewDoubleValue(float64(i)))
				require.NoError(t, err)
				enc := buf.Bytes()
				require.Equal(t, enc, v)
				i++
				return nil
			})
			require.Equal(t, 10, i)
			require.NoError(t, err)

			idx, err = tx.GetIndex("t2a")
			require.NoError(t, err)

			i = 0
			err = idx.AscendGreaterOrEqual([]document.Value{document.Value{Type: document.DoubleValue}}, func(v, k []byte) error {
				var buf bytes.Buffer
				err = document.NewValueEncoder(&buf).Encode(document.NewDoubleValue(float64(i)))
				require.NoError(t, err)
				enc := buf.Bytes()
				require.Equal(t, enc, v)
				i++
				return nil
			})
			require.Equal(t, 10, i)
			require.NoError(t, err)

			return errDontCommit
		})

		require.Equal(t, clone, catalog)
	})
}

func TestReadOnlyTables(t *testing.T) {
	db, err := genji.Open(":memory:")
	require.NoError(t, err)
	defer db.Close()

	doc, err := db.QueryDocument(`CREATE TABLE foo; SELECT * FROM __genji_tables`)
	require.NoError(t, err)

	testutil.RequireDocJSONEq(t, doc, `{"field_constraints": [], "read_only":false, "store_name":"dAE=", "table_name":"foo"}`)

	doc, err = db.QueryDocument(`CREATE INDEX idx_foo_a ON foo(a); SELECT * FROM __genji_indexes`)
	require.NoError(t, err)

	testutil.RequireDocJSONEq(t, doc, `{"index_name":"idx_foo_a", "path":["a"], "table_name":"foo", "unique":false}`)
}
