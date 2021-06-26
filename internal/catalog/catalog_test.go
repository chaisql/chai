package catalog_test

import (
	"bytes"
	"errors"
	"testing"

	"github.com/genjidb/genji"
	"github.com/genjidb/genji/document"
	errs "github.com/genjidb/genji/errors"
	"github.com/genjidb/genji/internal/catalog"
	"github.com/genjidb/genji/internal/database"
	"github.com/genjidb/genji/internal/testutil"
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

func cloneCatalog(c database.Catalog) database.Catalog {
	orig := c.(*catalog.Catalog)

	var clone catalog.Catalog

	clone.CatalogTable = orig.CatalogTable
	clone.Cache = orig.Cache.Clone()

	return &clone

}

// TestCatalogTable tests all basic operations on tables:
// - CreateTable
// - GetTable
// - DropTable
// - RenameTable
// - AddFieldConstraint
func TestCatalogTable(t *testing.T) {
	t.Run("Get", func(t *testing.T) {
		db, cleanup := testutil.NewTestDB(t)
		defer cleanup()

		update(t, db, func(tx *database.Transaction) error {
			return tx.Catalog.CreateTable(tx, "test", nil)
		})

		update(t, db, func(tx *database.Transaction) error {
			table, err := tx.Catalog.GetTable(tx, "test")
			require.NoError(t, err)
			require.Equal(t, "test", table.Name)

			// Getting a table that doesn't exist should fail.
			_, err = tx.Catalog.GetTable(tx, "unknown")
			if !errors.Is(err, errs.ErrTableNotFound) {
				require.Equal(t, err, errs.ErrTableNotFound)
			}

			return nil
		})
	})

	t.Run("Drop", func(t *testing.T) {
		db, cleanup := testutil.NewTestDB(t)
		defer cleanup()

		update(t, db, func(tx *database.Transaction) error {
			return tx.Catalog.CreateTable(tx, "test", nil)
		})

		clone := cloneCatalog(db.Catalog)

		update(t, db, func(tx *database.Transaction) error {
			err := tx.Catalog.DropTable(tx, "test")
			require.NoError(t, err)

			// Getting a table that has been dropped should fail.
			_, err = tx.Catalog.GetTable(tx, "test")
			if !errors.Is(err, errs.ErrTableNotFound) {
				require.Equal(t, err, errs.ErrTableNotFound)
			}

			// Dropping a table that doesn't exist should fail.
			err = tx.Catalog.DropTable(tx, "test")
			if !errors.Is(err, errs.ErrTableNotFound) {
				require.Equal(t, err, errs.ErrTableNotFound)
			}

			return errDontCommit
		})

		require.Equal(t, clone, db.Catalog)
	})

	t.Run("Rename", func(t *testing.T) {
		db, cleanup := testutil.NewTestDB(t)
		defer cleanup()

		ti := &database.TableInfo{FieldConstraints: []*database.FieldConstraint{
			{Path: testutil.ParseDocumentPath(t, "name"), Type: document.TextValue, IsNotNull: true},
			{Path: testutil.ParseDocumentPath(t, "age"), Type: document.IntegerValue, IsPrimaryKey: true},
			{Path: testutil.ParseDocumentPath(t, "gender"), Type: document.TextValue},
			{Path: testutil.ParseDocumentPath(t, "city"), Type: document.TextValue},
		}}

		update(t, db, func(tx *database.Transaction) error {
			err := tx.Catalog.CreateTable(tx, "foo", ti)
			require.NoError(t, err)

			err = tx.Catalog.CreateIndex(tx, &database.IndexInfo{Paths: []document.Path{testutil.ParseDocumentPath(t, "gender")}, IndexName: "idx_gender", TableName: "foo"})
			require.NoError(t, err)
			err = tx.Catalog.CreateIndex(tx, &database.IndexInfo{Paths: []document.Path{testutil.ParseDocumentPath(t, "city")}, IndexName: "idx_city", TableName: "foo", Unique: true})
			require.NoError(t, err)

			return nil
		})

		clone := cloneCatalog(db.Catalog)

		update(t, db, func(tx *database.Transaction) error {
			err := tx.Catalog.RenameTable(tx, "foo", "zoo")
			require.NoError(t, err)

			// Getting the old table should return an error.
			_, err = tx.Catalog.GetTable(tx, "foo")
			if !errors.Is(err, errs.ErrTableNotFound) {
				require.Equal(t, err, errs.ErrTableNotFound)
			}

			tb, err := tx.Catalog.GetTable(tx, "zoo")
			require.NoError(t, err)
			// The field constraints should be the same.

			require.Equal(t, ti.FieldConstraints, tb.Info.FieldConstraints)

			// Check that the indexes have been updated as well.
			idxs := tx.Catalog.ListIndexes(tb.Name)
			require.Len(t, idxs, 2)
			for _, name := range idxs {
				idx, err := tx.Catalog.GetIndex(tx, name)
				require.NoError(t, err)
				require.Equal(t, "zoo", idx.Info.TableName)
			}

			// Renaming a non existing table should return an error
			err = tx.Catalog.RenameTable(tx, "foo", "")
			if !errors.Is(err, errs.ErrTableNotFound) {
				require.Equal(t, err, errs.ErrTableNotFound)
			}

			return errDontCommit
		})

		require.Equal(t, clone, db.Catalog)
	})

	t.Run("Add field constraint", func(t *testing.T) {
		db, cleanup := testutil.NewTestDB(t)
		defer cleanup()

		ti := &database.TableInfo{FieldConstraints: []*database.FieldConstraint{
			{Path: testutil.ParseDocumentPath(t, "name"), Type: document.TextValue, IsNotNull: true},
			{Path: testutil.ParseDocumentPath(t, "age"), Type: document.IntegerValue, IsPrimaryKey: true},
			{Path: testutil.ParseDocumentPath(t, "gender"), Type: document.TextValue},
			{Path: testutil.ParseDocumentPath(t, "city"), Type: document.TextValue},
		}}

		update(t, db, func(tx *database.Transaction) error {
			return tx.Catalog.CreateTable(tx, "foo", ti)
		})

		clone := cloneCatalog(db.Catalog)

		update(t, db, func(tx *database.Transaction) error {

			// Add field constraint
			fieldToAdd := database.FieldConstraint{
				Path: testutil.ParseDocumentPath(t, "last_name"), Type: document.TextValue,
			}
			err := tx.Catalog.AddFieldConstraint(tx, "foo", fieldToAdd)
			require.NoError(t, err)

			tb, err := tx.Catalog.GetTable(tx, "foo")
			require.NoError(t, err)

			// The field constraints should not be the same.

			require.Contains(t, tb.Info.FieldConstraints, &fieldToAdd)

			// Renaming a non existing table should return an error
			err = tx.Catalog.AddFieldConstraint(tx, "bar", fieldToAdd)
			if !errors.Is(err, errs.ErrTableNotFound) {
				require.Equal(t, err, errs.ErrTableNotFound)
			}

			// Adding a existing field should return an error
			err = tx.Catalog.AddFieldConstraint(tx, "foo", *ti.FieldConstraints[0])
			require.Error(t, err)

			// Adding a second primary key should return an error
			fieldToAdd = database.FieldConstraint{
				Path: testutil.ParseDocumentPath(t, "foobar"), Type: document.IntegerValue, IsPrimaryKey: true,
			}
			err = tx.Catalog.AddFieldConstraint(tx, "foo", fieldToAdd)
			require.Error(t, err)

			return errDontCommit
		})

		require.Equal(t, clone, db.Catalog)
	})
}

func TestCatalogCreateTable(t *testing.T) {
	t.Run("Same table name", func(t *testing.T) {
		db, cleanup := testutil.NewTestDB(t)
		defer cleanup()

		clone := cloneCatalog(db.Catalog)

		update(t, db, func(tx *database.Transaction) error {
			err := tx.Catalog.CreateTable(tx, "test", nil)
			require.NoError(t, err)

			// Creating a table that already exists should fail.
			err = tx.Catalog.CreateTable(tx, "test", nil)
			require.Equal(t, err, errs.AlreadyExistsError{Name: "test"})

			return errDontCommit
		})

		require.Equal(t, clone, db.Catalog)
	})

	t.Run("Create and rollback", func(t *testing.T) {
		db, cleanup := testutil.NewTestDB(t)
		defer cleanup()

		check := func() {
			update(t, db, func(tx *database.Transaction) error {
				err := tx.Catalog.CreateTable(tx, "test", nil)
				require.NoError(t, err)

				return errDontCommit
			})
		}

		check()
		check()
	})

	t.Run("Invalid constraints", func(t *testing.T) {
		db, cleanup := testutil.NewTestDB(t)
		defer cleanup()

		clone := cloneCatalog(db.Catalog)

		update(t, db, func(tx *database.Transaction) error {
			err := tx.Catalog.CreateTable(tx, "test", &database.TableInfo{
				FieldConstraints: []*database.FieldConstraint{
					{Path: document.NewPath("a", "b"), Type: document.IntegerValue},
					{Path: document.NewPath("a"), Type: document.IntegerValue},
				},
			})
			require.Error(t, err)
			return errDontCommit
		})

		require.Equal(t, clone, db.Catalog)
	})
}

// values is a helper function to avoid having to type []document.Value{} all the time.
func values(vs ...document.Value) []document.Value {
	return vs
}

func TestCatalogCreateIndex(t *testing.T) {
	t.Run("Should create an index, build it and return it", func(t *testing.T) {
		db, cleanup := testutil.NewTestDB(t)
		defer cleanup()

		update(t, db, func(tx *database.Transaction) error {
			err := tx.Catalog.CreateTable(tx, "test", nil)
			if err != nil {
				return err
			}

			tb, err := tx.Catalog.GetTable(tx, "test")
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

		clone := cloneCatalog(db.Catalog)

		update(t, db, func(tx *database.Transaction) error {
			err := tx.Catalog.CreateIndex(tx, &database.IndexInfo{
				IndexName: "idx_a", TableName: "test", Paths: []document.Path{testutil.ParseDocumentPath(t, "a")},
			})
			require.NoError(t, err)
			idx, err := tx.Catalog.GetIndex(tx, "idx_a")
			require.NoError(t, err)
			require.NotNil(t, idx)

			var i int
			err = idx.AscendGreaterOrEqual(values(document.Value{Type: document.DoubleValue}), func(v, k []byte) error {
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

		require.Equal(t, clone, db.Catalog)
	})

	t.Run("Should fail if it already exists", func(t *testing.T) {
		db, cleanup := testutil.NewTestDB(t)
		defer cleanup()

		update(t, db, func(tx *database.Transaction) error {
			return tx.Catalog.CreateTable(tx, "test", nil)
		})

		update(t, db, func(tx *database.Transaction) error {
			err := tx.Catalog.CreateIndex(tx, &database.IndexInfo{
				IndexName: "idxFoo", TableName: "test", Paths: []document.Path{testutil.ParseDocumentPath(t, "foo")},
			})
			require.NoError(t, err)

			err = tx.Catalog.CreateIndex(tx, &database.IndexInfo{
				IndexName: "idxFoo", TableName: "test", Paths: []document.Path{testutil.ParseDocumentPath(t, "foo")},
			})
			require.Equal(t, errs.AlreadyExistsError{Name: "idxFoo"}, err)
			return nil
		})
	})

	t.Run("Should fail if table doesn't exist", func(t *testing.T) {
		db, cleanup := testutil.NewTestDB(t)
		defer cleanup()
		update(t, db, func(tx *database.Transaction) error {
			err := tx.Catalog.CreateIndex(tx, &database.IndexInfo{
				IndexName: "idxFoo", TableName: "test", Paths: []document.Path{testutil.ParseDocumentPath(t, "foo")},
			})
			if !errors.Is(err, errs.ErrTableNotFound) {
				require.Equal(t, err, errs.ErrTableNotFound)
			}

			return nil
		})
	})

	t.Run("Should generate a name if not provided", func(t *testing.T) {
		db, cleanup := testutil.NewTestDB(t)
		defer cleanup()

		update(t, db, func(tx *database.Transaction) error {
			return tx.Catalog.CreateTable(tx, "test", nil)
		})

		update(t, db, func(tx *database.Transaction) error {
			err := tx.Catalog.CreateIndex(tx, &database.IndexInfo{
				TableName: "test", Paths: []document.Path{testutil.ParseDocumentPath(t, "foo.a[10].`  bar `.c")},
			})
			require.NoError(t, err)

			_, err = tx.Catalog.GetIndex(tx, "test_foo.a[10].  bar .c_idx")
			require.NoError(t, err)

			// create another one
			err = tx.Catalog.CreateIndex(tx, &database.IndexInfo{
				TableName: "test", Paths: []document.Path{testutil.ParseDocumentPath(t, "foo.a[10].`  bar `.c")},
			})
			require.NoError(t, err)

			_, err = tx.Catalog.GetIndex(tx, "test_foo.a[10].  bar .c_idx1")
			require.NoError(t, err)
			return nil
		})
	})
}

func TestTxDropIndex(t *testing.T) {
	t.Run("Should drop an index", func(t *testing.T) {
		db, cleanup := testutil.NewTestDB(t)
		defer cleanup()

		update(t, db, func(tx *database.Transaction) error {
			err := tx.Catalog.CreateTable(tx, "test", nil)
			require.NoError(t, err)
			err = tx.Catalog.CreateIndex(tx, &database.IndexInfo{
				IndexName: "idxFoo", TableName: "test", Paths: []document.Path{testutil.ParseDocumentPath(t, "foo")},
			})
			require.NoError(t, err)
			err = tx.Catalog.CreateIndex(tx, &database.IndexInfo{
				IndexName: "idxBar", TableName: "test", Paths: []document.Path{testutil.ParseDocumentPath(t, "bar")},
			})
			require.NoError(t, err)
			return nil
		})

		clone := cloneCatalog(db.Catalog)
		update(t, db, func(tx *database.Transaction) error {
			err := tx.Catalog.DropIndex(tx, "idxFoo")
			require.NoError(t, err)

			_, err = tx.Catalog.GetIndex(tx, "idxFoo")
			require.Error(t, err)

			_, err = tx.Catalog.GetIndex(tx, "idxBar")
			require.NoError(t, err)

			// cf: https://github.com/genjidb/genji/issues/360
			_, err = tx.Catalog.GetTable(tx, "test")
			require.NoError(t, err)

			return errDontCommit
		})

		require.Equal(t, clone, db.Catalog)
	})

	t.Run("Should fail if it doesn't exist", func(t *testing.T) {
		db, cleanup := testutil.NewTestDB(t)
		defer cleanup()

		update(t, db, func(tx *database.Transaction) error {
			err := tx.Catalog.DropIndex(tx, "idxFoo")
			require.Equal(t, errs.ErrIndexNotFound, err)
			return nil
		})
	})
}

func TestCatalogReIndex(t *testing.T) {
	prepareTableFn := func(t *testing.T, db *database.Database) {

		update(t, db, func(tx *database.Transaction) error {
			err := tx.Catalog.CreateTable(tx, "test", nil)
			require.NoError(t, err)
			tb, err := tx.Catalog.GetTable(tx, "test")
			require.NoError(t, err)

			for i := int64(0); i < 10; i++ {
				_, err = tb.Insert(document.NewFieldBuffer().
					Add("a", document.NewIntegerValue(i)).
					Add("b", document.NewIntegerValue(i*10)),
				)
				require.NoError(t, err)
			}

			err = tx.Catalog.CreateIndex(tx, &database.IndexInfo{
				IndexName: "a",
				TableName: "test",
				Paths:     []document.Path{testutil.ParseDocumentPath(t, "a")},
			})
			require.NoError(t, err)
			err = tx.Catalog.CreateIndex(tx, &database.IndexInfo{
				IndexName: "b",
				TableName: "test",
				Paths:     []document.Path{testutil.ParseDocumentPath(t, "b")},
			})
			require.NoError(t, err)

			return nil
		})
	}

	t.Run("Should fail if not found", func(t *testing.T) {
		db, cleanup := testutil.NewTestDB(t)
		defer cleanup()

		prepareTableFn(t, db)

		update(t, db, func(tx *database.Transaction) error {
			err := tx.Catalog.ReIndex(tx, "foo")
			require.Equal(t, errs.ErrIndexNotFound, err)
			return nil
		})
	})

	t.Run("Should not fail if field not found", func(t *testing.T) {
		db, cleanup := testutil.NewTestDB(t)
		defer cleanup()

		update(t, db, func(tx *database.Transaction) error {
			err := tx.Catalog.CreateTable(tx, "test", nil)
			require.NoError(t, err)

			tb, err := tx.Catalog.GetTable(tx, "test")
			require.NoError(t, err)

			_, err = tb.Insert(document.NewFieldBuffer().
				Add("a", document.NewIntegerValue(1)),
			)
			require.NoError(t, err)

			return tx.Catalog.CreateIndex(tx, &database.IndexInfo{
				IndexName: "b",
				TableName: "test",
				Paths:     []document.Path{testutil.ParseDocumentPath(t, "b")},
			})
		})

		clone := cloneCatalog(db.Catalog)

		update(t, db, func(tx *database.Transaction) error {
			err := tx.Catalog.ReIndex(tx, "b")
			require.NoError(t, err)

			return errDontCommit
		})

		require.Equal(t, clone, db.Catalog)
	})

	t.Run("Should reindex the index", func(t *testing.T) {
		db, cleanup := testutil.NewTestDB(t)
		defer cleanup()

		prepareTableFn(t, db)

		clone := cloneCatalog(db.Catalog)

		update(t, db, func(tx *database.Transaction) error {
			err := tx.Catalog.ReIndex(tx, "a")
			require.NoError(t, err)

			idx, err := tx.Catalog.GetIndex(tx, "a")
			require.NoError(t, err)

			var i int
			err = idx.AscendGreaterOrEqual([]document.Value{{Type: document.DoubleValue}}, func(v, k []byte) error {
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

			_, err = tx.Catalog.GetIndex(tx, "b")
			require.NoError(t, err)

			return errDontCommit
		})

		require.Equal(t, clone, db.Catalog)
	})
}

func TestReIndexAll(t *testing.T) {
	t.Run("Should succeed if no indexes", func(t *testing.T) {
		db, cleanup := testutil.NewTestDB(t)
		defer cleanup()

		update(t, db, func(tx *database.Transaction) error {
			err := tx.Catalog.ReIndexAll(tx)
			require.NoError(t, err)
			return nil
		})
	})

	t.Run("Should reindex all indexes", func(t *testing.T) {
		db, cleanup := testutil.NewTestDB(t)
		defer cleanup()

		update(t, db, func(tx *database.Transaction) error {
			err := tx.Catalog.CreateTable(tx, "test1", nil)
			require.NoError(t, err)
			tb1, err := tx.Catalog.GetTable(tx, "test1")
			require.NoError(t, err)

			err = tx.Catalog.CreateTable(tx, "test2", nil)
			require.NoError(t, err)
			tb2, err := tx.Catalog.GetTable(tx, "test2")
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

			err = tx.Catalog.CreateIndex(tx, &database.IndexInfo{
				IndexName: "t1a",
				TableName: "test1",
				Paths:     []document.Path{testutil.ParseDocumentPath(t, "a")},
			})
			require.NoError(t, err)
			err = tx.Catalog.CreateIndex(tx, &database.IndexInfo{
				IndexName: "t2a",
				TableName: "test2",
				Paths:     []document.Path{testutil.ParseDocumentPath(t, "a")},
			})
			require.NoError(t, err)

			return nil
		})

		clone := cloneCatalog(db.Catalog)

		update(t, db, func(tx *database.Transaction) error {
			err := tx.Catalog.ReIndexAll(tx)
			require.NoError(t, err)
			idx, err := tx.Catalog.GetIndex(tx, "t1a")
			require.NoError(t, err)

			var i int
			err = idx.AscendGreaterOrEqual([]document.Value{{Type: document.DoubleValue}}, func(v, k []byte) error {
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

			idx, err = tx.Catalog.GetIndex(tx, "t2a")
			require.NoError(t, err)

			i = 0
			err = idx.AscendGreaterOrEqual([]document.Value{{Type: document.DoubleValue}}, func(v, k []byte) error {
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

		require.Equal(t, clone, db.Catalog)
	})
}

func TestReadOnlyTables(t *testing.T) {
	db, err := genji.Open(":memory:")
	require.NoError(t, err)
	defer db.Close()

	res, err := db.Query(`
		CREATE TABLE foo (a int, b[3].c double unique);
		CREATE INDEX idx_foo_a ON foo(a);
		SELECT * FROM __genji_catalog
	`)
	require.NoError(t, err)
	defer res.Close()

	var i int
	err = res.Iterate(func(d document.Document) error {
		switch i {
		case 0:
			testutil.RequireDocJSONEq(t, d, `{"name":"foo", "type":"table", "store_name":"AQ==", "sql":"CREATE TABLE foo (a INTEGER, b[3].c DOUBLE UNIQUE)"}`)
		case 1:
			testutil.RequireDocJSONEq(t, d, `{"constraint_path":"b[3].c", "name":"foo_b[3].c_idx", "sql":"CREATE UNIQUE INDEX foo_b[3].c_idx ON foo (b[3].c)", "store_name":"Ag==", "table_name":"foo", "type":"index"}`)
		case 2:
			testutil.RequireDocJSONEq(t, d, `{"name":"idx_foo_a", "sql":"CREATE INDEX idx_foo_a ON foo (a)", "store_name":"Aw==", "table_name":"foo", "type":"index"}`)
		default:
			t.Fatalf("count should be 2, got %d", i)
		}

		i++
		return nil
	})
	require.NoError(t, err)
}

func TestCatalogCreateSequence(t *testing.T) {
	t.Run("Should create a sequence and add it to the schema and sequence tables", func(t *testing.T) {
		db, cleanup := testutil.NewTestDB(t)
		defer cleanup()

		update(t, db, func(tx *database.Transaction) error {
			err := tx.Catalog.CreateSequence(tx, "test1", &database.SequenceInfo{Name: "test1", IncrementBy: 1})
			if err != nil {
				return err
			}

			seq, err := tx.Catalog.GetSequence("test1")
			require.NoError(t, err)
			require.NotNil(t, seq)

			_, err = db.Catalog.(*catalog.Catalog).CatalogTable.GetTable(tx).GetDocument([]byte("test1"))
			require.NoError(t, err)

			tb, err := db.Catalog.GetTable(tx, database.SequenceTableName)
			require.NoError(t, err)

			_, err = tb.GetDocument([]byte("test1"))
			require.NoError(t, err)
			return nil
		})

		clone := cloneCatalog(db.Catalog)

		update(t, db, func(tx *database.Transaction) error {
			err := tx.Catalog.CreateSequence(tx, "test2", &database.SequenceInfo{Name: "test2", IncrementBy: 1})
			if err != nil {
				return err
			}
			seq, err := tx.Catalog.GetSequence("test2")
			require.NoError(t, err)
			require.NotNil(t, seq)

			return errDontCommit
		})

		require.Equal(t, clone, db.Catalog)
	})

	t.Run("Should fail if it already exists", func(t *testing.T) {
		db, cleanup := testutil.NewTestDB(t)
		defer cleanup()

		update(t, db, func(tx *database.Transaction) error {
			return tx.Catalog.CreateSequence(tx, "test", nil)
		})

		update(t, db, func(tx *database.Transaction) error {
			err := tx.Catalog.CreateSequence(tx, "test", nil)
			require.Equal(t, errs.AlreadyExistsError{Name: "test"}, err)
			return nil
		})
	})
}
