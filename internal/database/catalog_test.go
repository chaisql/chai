package database_test

import (
	"bytes"
	"fmt"
	"math"
	"testing"

	"github.com/genjidb/genji"
	"github.com/genjidb/genji/document"
	errs "github.com/genjidb/genji/errors"
	"github.com/genjidb/genji/internal/database"
	"github.com/genjidb/genji/internal/errors"
	"github.com/genjidb/genji/internal/testutil"
	"github.com/genjidb/genji/internal/testutil/assert"
	"github.com/genjidb/genji/types"
	"github.com/stretchr/testify/require"
)

func updateCatalog(t testing.TB, db *database.Database, fn func(tx *database.Transaction, catalog *database.Catalog) error) {
	t.Helper()

	tx, err := db.Begin(true)
	assert.NoError(t, err)
	defer tx.Rollback()

	err = fn(tx, db.Catalog)
	if errors.Is(err, errDontCommit) {
		tx.Rollback()
		return
	}
	assert.NoError(t, err)

	err = tx.Commit()
	assert.NoError(t, err)
}

func cloneCatalog(c *database.Catalog) *database.Catalog {
	var clone database.Catalog

	clone.CatalogTable = c.CatalogTable
	clone.Codec = c.Codec
	clone.Cache = c.Cache.Clone()

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

		updateCatalog(t, db, func(tx *database.Transaction, catalog *database.Catalog) error {
			return catalog.CreateTable(tx, "test", nil)
		})

		updateCatalog(t, db, func(tx *database.Transaction, catalog *database.Catalog) error {
			table, err := catalog.GetTable(tx, "test")
			assert.NoError(t, err)
			require.Equal(t, "test", table.Info.Name())

			// Getting a table that doesn't exist should fail.
			_, err = catalog.GetTable(tx, "unknown")
			if !errors.Is(err, errs.NotFoundError{}) {
				assert.ErrorIs(t, err, errs.NotFoundError{Name: "unknown"})
			}

			return nil
		})
	})

	t.Run("Drop", func(t *testing.T) {
		db, cleanup := testutil.NewTestDB(t)
		defer cleanup()

		updateCatalog(t, db, func(tx *database.Transaction, catalog *database.Catalog) error {
			return catalog.CreateTable(tx, "test", nil)
		})

		clone := cloneCatalog(db.Catalog)

		updateCatalog(t, db, func(tx *database.Transaction, catalog *database.Catalog) error {
			err := catalog.DropTable(tx, "test")
			assert.NoError(t, err)

			// Getting a table that has been dropped should fail.
			_, err = catalog.GetTable(tx, "test")
			if !errors.Is(err, errs.NotFoundError{}) {
				assert.ErrorIs(t, err, errs.NotFoundError{Name: "test"})
			}

			// Dropping a table that doesn't exist should fail.
			err = catalog.DropTable(tx, "test")
			if !errors.Is(err, errs.NotFoundError{}) {
				assert.ErrorIs(t, err, errs.NotFoundError{Name: "test"})
			}

			return errDontCommit
		})

		require.Equal(t, clone, db.Catalog)
	})

	t.Run("Rename", func(t *testing.T) {
		db, cleanup := testutil.NewTestDB(t)
		defer cleanup()

		ti := &database.TableInfo{FieldConstraints: []*database.FieldConstraint{
			{Path: testutil.ParseDocumentPath(t, "name"), Type: types.TextValue, IsNotNull: true},
			{Path: testutil.ParseDocumentPath(t, "age"), Type: types.IntegerValue, IsPrimaryKey: true},
			{Path: testutil.ParseDocumentPath(t, "gender"), Type: types.TextValue},
			{Path: testutil.ParseDocumentPath(t, "city"), Type: types.TextValue},
		}}

		updateCatalog(t, db, func(tx *database.Transaction, catalog *database.Catalog) error {
			err := catalog.CreateTable(tx, "foo", ti)
			assert.NoError(t, err)

			err = catalog.CreateIndex(tx, &database.IndexInfo{Paths: []document.Path{testutil.ParseDocumentPath(t, "gender")}, IndexName: "idx_gender", TableName: "foo"})
			assert.NoError(t, err)
			err = catalog.CreateIndex(tx, &database.IndexInfo{Paths: []document.Path{testutil.ParseDocumentPath(t, "city")}, IndexName: "idx_city", TableName: "foo", Unique: true})
			assert.NoError(t, err)

			seq := database.SequenceInfo{
				Name:        "seq_foo",
				IncrementBy: 1,
				Min:         1, Max: math.MaxInt64,
				Start: 1,
				Cache: 64,
				Owner: database.Owner{
					TableName: "foo",
				},
			}
			err = catalog.CreateSequence(tx, &seq)
			assert.NoError(t, err)

			return nil
		})

		clone := cloneCatalog(db.Catalog)

		updateCatalog(t, db, func(tx *database.Transaction, catalog *database.Catalog) error {
			err := catalog.RenameTable(tx, "foo", "zoo")
			assert.NoError(t, err)

			// Getting the old table should return an error.
			_, err = catalog.GetTable(tx, "foo")
			if !errors.Is(err, errs.NotFoundError{}) {
				assert.ErrorIs(t, err, errs.NotFoundError{Name: "foo"})
			}

			tb, err := catalog.GetTable(tx, "zoo")
			assert.NoError(t, err)
			// The field constraints should be the same.

			require.Equal(t, ti.FieldConstraints, tb.Info.FieldConstraints)

			// Check that the indexes have been updated as well.
			idxs := catalog.ListIndexes(tb.Info.Name())
			require.Len(t, idxs, 2)
			for _, name := range idxs {
				idx, err := catalog.GetIndex(tx, name)
				assert.NoError(t, err)
				require.Equal(t, "zoo", idx.Info.TableName)
			}

			// Check that the sequences have been updated as well.
			seq, err := catalog.GetSequence("seq_foo")
			assert.NoError(t, err)
			require.Equal(t, "zoo", seq.Info.Owner.TableName)

			// Renaming a non existing table should return an error
			err = catalog.RenameTable(tx, "foo", "")
			if !errors.Is(err, errs.NotFoundError{}) {
				assert.ErrorIs(t, err, errs.NotFoundError{Name: "foo"})
			}

			return errDontCommit
		})

		require.Equal(t, clone, db.Catalog)
	})

	t.Run("Add field constraint", func(t *testing.T) {
		db, cleanup := testutil.NewTestDB(t)
		defer cleanup()

		ti := &database.TableInfo{FieldConstraints: []*database.FieldConstraint{
			{Path: testutil.ParseDocumentPath(t, "name"), Type: types.TextValue, IsNotNull: true},
			{Path: testutil.ParseDocumentPath(t, "age"), Type: types.IntegerValue, IsPrimaryKey: true},
			{Path: testutil.ParseDocumentPath(t, "gender"), Type: types.TextValue},
			{Path: testutil.ParseDocumentPath(t, "city"), Type: types.TextValue},
		}}

		updateCatalog(t, db, func(tx *database.Transaction, catalog *database.Catalog) error {
			return catalog.CreateTable(tx, "foo", ti)
		})

		clone := cloneCatalog(db.Catalog)

		updateCatalog(t, db, func(tx *database.Transaction, catalog *database.Catalog) error {

			// Add field constraint
			fieldToAdd := database.FieldConstraint{
				Path: testutil.ParseDocumentPath(t, "last_name"), Type: types.TextValue,
			}
			err := catalog.AddFieldConstraint(tx, "foo", fieldToAdd)
			assert.NoError(t, err)

			tb, err := catalog.GetTable(tx, "foo")
			assert.NoError(t, err)

			// The field constraints should not be the same.

			require.Contains(t, tb.Info.FieldConstraints, &fieldToAdd)

			// Renaming a non existing table should return an error
			err = catalog.AddFieldConstraint(tx, "bar", fieldToAdd)
			if !errors.Is(err, errs.NotFoundError{}) {
				assert.ErrorIs(t, err, errs.NotFoundError{Name: "bar"})
			}

			// Adding a existing field should return an error
			err = catalog.AddFieldConstraint(tx, "foo", *ti.FieldConstraints[0])
			assert.Error(t, err)

			// Adding a second primary key should return an error
			fieldToAdd = database.FieldConstraint{
				Path: testutil.ParseDocumentPath(t, "foobar"), Type: types.IntegerValue, IsPrimaryKey: true,
			}
			err = catalog.AddFieldConstraint(tx, "foo", fieldToAdd)
			assert.Error(t, err)

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

		updateCatalog(t, db, func(tx *database.Transaction, catalog *database.Catalog) error {
			err := catalog.CreateTable(tx, "test", nil)
			assert.NoError(t, err)

			// Creating a table that already exists should fail.
			err = catalog.CreateTable(tx, "test", nil)
			assert.ErrorIs(t, err, errs.AlreadyExistsError{Name: "test"})

			return errDontCommit
		})

		require.Equal(t, clone, db.Catalog)
	})

	t.Run("Create and rollback", func(t *testing.T) {
		db, cleanup := testutil.NewTestDB(t)
		defer cleanup()

		check := func() {
			updateCatalog(t, db, func(tx *database.Transaction, catalog *database.Catalog) error {
				err := catalog.CreateTable(tx, "test", nil)
				assert.NoError(t, err)

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

		updateCatalog(t, db, func(tx *database.Transaction, catalog *database.Catalog) error {
			err := catalog.CreateTable(tx, "test", &database.TableInfo{
				FieldConstraints: []*database.FieldConstraint{
					{Path: document.NewPath("a", "b"), Type: types.IntegerValue},
					{Path: document.NewPath("a"), Type: types.IntegerValue},
				},
			})
			assert.Error(t, err)
			return errDontCommit
		})

		require.Equal(t, clone, db.Catalog)
	})
}

func TestCatalogCreateIndex(t *testing.T) {
	t.Run("Should create an index, build it and return it", func(t *testing.T) {
		db, cleanup := testutil.NewTestDB(t)
		defer cleanup()

		updateCatalog(t, db, func(tx *database.Transaction, catalog *database.Catalog) error {
			err := catalog.CreateTable(tx, "test", &database.TableInfo{
				FieldConstraints: database.FieldConstraints{
					{Path: testutil.ParseDocumentPath(t, "a"), IsPrimaryKey: true},
				},
			})
			if err != nil {
				return err
			}

			tb, err := catalog.GetTable(tx, "test")
			assert.NoError(t, err)

			for i := int64(0); i < 10; i++ {
				_, err = tb.Insert(document.NewFieldBuffer().
					Add("a", types.NewIntegerValue(i)).
					Add("b", types.NewIntegerValue(i*10)),
				)
				assert.NoError(t, err)
			}

			return nil
		})

		clone := cloneCatalog(db.Catalog)

		updateCatalog(t, db, func(tx *database.Transaction, catalog *database.Catalog) error {
			err := catalog.CreateIndex(tx, &database.IndexInfo{
				IndexName: "idx_a", TableName: "test", Paths: []document.Path{testutil.ParseDocumentPath(t, "a")},
			})
			assert.NoError(t, err)
			idx, err := catalog.GetIndex(tx, "idx_a")
			assert.NoError(t, err)
			require.NotNil(t, idx)

			var i int
			err = idx.AscendGreaterOrEqual(values(types.NewEmptyValue(types.DoubleValue)), func(v, k []byte) error {
				var buf bytes.Buffer
				err = types.NewValueEncoder(&buf).Encode(
					types.NewArrayValue(
						document.NewValueBuffer(
							types.NewDoubleValue(float64(i)),
						),
					),
				)
				assert.NoError(t, err)
				enc := buf.Bytes()
				require.Equal(t, enc, v)
				i++
				return nil
			})
			require.Equal(t, 10, i)
			assert.NoError(t, err)

			return errDontCommit
		})

		require.Equal(t, clone, db.Catalog)
	})

	t.Run("Should fail if it already exists", func(t *testing.T) {
		db, cleanup := testutil.NewTestDB(t)
		defer cleanup()

		updateCatalog(t, db, func(tx *database.Transaction, catalog *database.Catalog) error {
			return catalog.CreateTable(tx, "test", nil)
		})

		updateCatalog(t, db, func(tx *database.Transaction, catalog *database.Catalog) error {
			err := catalog.CreateIndex(tx, &database.IndexInfo{
				IndexName: "idxFoo", TableName: "test", Paths: []document.Path{testutil.ParseDocumentPath(t, "foo")},
			})
			assert.NoError(t, err)

			err = catalog.CreateIndex(tx, &database.IndexInfo{
				IndexName: "idxFoo", TableName: "test", Paths: []document.Path{testutil.ParseDocumentPath(t, "foo")},
			})
			assert.ErrorIs(t, err, errs.AlreadyExistsError{Name: "idxFoo"})
			return nil
		})
	})

	t.Run("Should fail if table doesn't exist", func(t *testing.T) {
		db, cleanup := testutil.NewTestDB(t)
		defer cleanup()
		updateCatalog(t, db, func(tx *database.Transaction, catalog *database.Catalog) error {
			err := catalog.CreateIndex(tx, &database.IndexInfo{
				IndexName: "idxFoo", TableName: "test", Paths: []document.Path{testutil.ParseDocumentPath(t, "foo")},
			})
			if !errors.Is(err, errs.NotFoundError{}) {
				assert.ErrorIs(t, err, errs.NotFoundError{Name: "test"})
			}

			return nil
		})
	})

	t.Run("Should generate a name if not provided", func(t *testing.T) {
		db, cleanup := testutil.NewTestDB(t)
		defer cleanup()

		updateCatalog(t, db, func(tx *database.Transaction, catalog *database.Catalog) error {
			return catalog.CreateTable(tx, "test", nil)
		})

		updateCatalog(t, db, func(tx *database.Transaction, catalog *database.Catalog) error {
			err := catalog.CreateIndex(tx, &database.IndexInfo{
				TableName: "test", Paths: []document.Path{testutil.ParseDocumentPath(t, "foo.a[10].`  bar `.c")},
			})
			assert.NoError(t, err)

			_, err = catalog.GetIndex(tx, "test_foo.a[10].  bar .c_idx")
			assert.NoError(t, err)

			// create another one
			err = catalog.CreateIndex(tx, &database.IndexInfo{
				TableName: "test", Paths: []document.Path{testutil.ParseDocumentPath(t, "foo.a[10].`  bar `.c")},
			})
			assert.NoError(t, err)

			_, err = catalog.GetIndex(tx, "test_foo.a[10].  bar .c_idx1")
			assert.NoError(t, err)
			return nil
		})
	})
}

func TestTxDropIndex(t *testing.T) {
	t.Run("Should drop an index", func(t *testing.T) {
		db, cleanup := testutil.NewTestDB(t)
		defer cleanup()

		updateCatalog(t, db, func(tx *database.Transaction, catalog *database.Catalog) error {
			err := catalog.CreateTable(tx, "test", nil)
			assert.NoError(t, err)
			err = catalog.CreateIndex(tx, &database.IndexInfo{
				IndexName: "idxFoo", TableName: "test", Paths: []document.Path{testutil.ParseDocumentPath(t, "foo")},
			})
			assert.NoError(t, err)
			err = catalog.CreateIndex(tx, &database.IndexInfo{
				IndexName: "idxBar", TableName: "test", Paths: []document.Path{testutil.ParseDocumentPath(t, "bar")},
			})
			assert.NoError(t, err)
			return nil
		})

		clone := cloneCatalog(db.Catalog)
		updateCatalog(t, db, func(tx *database.Transaction, catalog *database.Catalog) error {
			err := catalog.DropIndex(tx, "idxFoo")
			assert.NoError(t, err)

			_, err = catalog.GetIndex(tx, "idxFoo")
			assert.Error(t, err)

			_, err = catalog.GetIndex(tx, "idxBar")
			assert.NoError(t, err)

			// cf: https://github.com/genjidb/genji/issues/360
			_, err = catalog.GetTable(tx, "test")
			assert.NoError(t, err)

			return errDontCommit
		})

		require.Equal(t, clone, db.Catalog)
	})

	t.Run("Should fail if it doesn't exist", func(t *testing.T) {
		db, cleanup := testutil.NewTestDB(t)
		defer cleanup()

		updateCatalog(t, db, func(tx *database.Transaction, catalog *database.Catalog) error {
			err := catalog.DropIndex(tx, "idxFoo")
			assert.ErrorIs(t, err, errs.NotFoundError{Name: "idxFoo"})
			return nil
		})
	})
}

func TestCatalogReIndex(t *testing.T) {
	prepareTableFn := func(t *testing.T, db *database.Database) {

		updateCatalog(t, db, func(tx *database.Transaction, catalog *database.Catalog) error {
			err := catalog.CreateTable(tx, "test", &database.TableInfo{
				FieldConstraints: database.FieldConstraints{
					{Path: testutil.ParseDocumentPath(t, "a"), IsPrimaryKey: true},
				},
			})
			assert.NoError(t, err)
			tb, err := catalog.GetTable(tx, "test")
			assert.NoError(t, err)

			for i := int64(0); i < 10; i++ {
				_, err = tb.Insert(document.NewFieldBuffer().
					Add("a", types.NewIntegerValue(i)).
					Add("b", types.NewIntegerValue(i*10)),
				)
				assert.NoError(t, err)
			}

			err = catalog.CreateIndex(tx, &database.IndexInfo{
				IndexName: "a",
				TableName: "test",
				Paths:     []document.Path{testutil.ParseDocumentPath(t, "a")},
			})
			assert.NoError(t, err)
			err = catalog.CreateIndex(tx, &database.IndexInfo{
				IndexName: "b",
				TableName: "test",
				Paths:     []document.Path{testutil.ParseDocumentPath(t, "b")},
			})
			assert.NoError(t, err)

			return nil
		})
	}

	t.Run("Should fail if not found", func(t *testing.T) {
		db, cleanup := testutil.NewTestDB(t)
		defer cleanup()

		prepareTableFn(t, db)

		updateCatalog(t, db, func(tx *database.Transaction, catalog *database.Catalog) error {
			err := catalog.ReIndex(tx, "foo")
			assert.ErrorIs(t, err, errs.NotFoundError{Name: "foo"})
			return nil
		})
	})

	t.Run("Should not fail if field not found", func(t *testing.T) {
		db, cleanup := testutil.NewTestDB(t)
		defer cleanup()

		updateCatalog(t, db, func(tx *database.Transaction, catalog *database.Catalog) error {
			err := catalog.CreateTable(tx, "test", &database.TableInfo{
				FieldConstraints: database.FieldConstraints{
					{Path: testutil.ParseDocumentPath(t, "a"), IsPrimaryKey: true},
				},
			})
			assert.NoError(t, err)

			tb, err := catalog.GetTable(tx, "test")
			assert.NoError(t, err)

			_, err = tb.Insert(document.NewFieldBuffer().
				Add("a", types.NewIntegerValue(1)),
			)
			assert.NoError(t, err)

			return catalog.CreateIndex(tx, &database.IndexInfo{
				IndexName: "b",
				TableName: "test",
				Paths:     []document.Path{testutil.ParseDocumentPath(t, "b")},
			})
		})

		clone := cloneCatalog(db.Catalog)

		updateCatalog(t, db, func(tx *database.Transaction, catalog *database.Catalog) error {
			err := catalog.ReIndex(tx, "b")
			assert.NoError(t, err)

			return errDontCommit
		})

		require.Equal(t, clone, db.Catalog)
	})

	t.Run("Should reindex the index", func(t *testing.T) {
		db, cleanup := testutil.NewTestDB(t)
		defer cleanup()

		prepareTableFn(t, db)

		clone := cloneCatalog(db.Catalog)

		updateCatalog(t, db, func(tx *database.Transaction, catalog *database.Catalog) error {
			err := catalog.ReIndex(tx, "a")
			assert.NoError(t, err)

			idx, err := catalog.GetIndex(tx, "a")
			assert.NoError(t, err)

			var i int
			err = idx.AscendGreaterOrEqual([]types.Value{types.NewEmptyValue(types.DoubleValue)}, func(v, k []byte) error {
				var buf bytes.Buffer
				err = types.NewValueEncoder(&buf).Encode(
					types.NewArrayValue(
						document.NewValueBuffer(
							types.NewDoubleValue(float64(i)),
						),
					),
				)
				assert.NoError(t, err)
				enc := buf.Bytes()
				require.Equal(t, enc, v)
				i++
				return nil
			})
			require.Equal(t, 10, i)
			assert.NoError(t, err)

			_, err = catalog.GetIndex(tx, "b")
			assert.NoError(t, err)

			return errDontCommit
		})

		require.Equal(t, clone, db.Catalog)
	})
}

func TestReIndexAll(t *testing.T) {
	t.Run("Should succeed if no indexes", func(t *testing.T) {
		db, cleanup := testutil.NewTestDB(t)
		defer cleanup()

		updateCatalog(t, db, func(tx *database.Transaction, catalog *database.Catalog) error {
			err := catalog.ReIndexAll(tx)
			assert.NoError(t, err)
			return nil
		})
	})

	t.Run("Should reindex all indexes", func(t *testing.T) {
		db, cleanup := testutil.NewTestDB(t)
		defer cleanup()

		updateCatalog(t, db, func(tx *database.Transaction, catalog *database.Catalog) error {
			err := catalog.CreateTable(tx, "test1", &database.TableInfo{
				FieldConstraints: database.FieldConstraints{
					{Path: testutil.ParseDocumentPath(t, "a"), IsPrimaryKey: true},
				},
			})
			assert.NoError(t, err)
			tb1, err := catalog.GetTable(tx, "test1")
			assert.NoError(t, err)

			err = catalog.CreateTable(tx, "test2", &database.TableInfo{
				FieldConstraints: database.FieldConstraints{
					{Path: testutil.ParseDocumentPath(t, "a"), IsPrimaryKey: true},
				},
			})
			assert.NoError(t, err)
			tb2, err := catalog.GetTable(tx, "test2")
			assert.NoError(t, err)

			for i := int64(0); i < 10; i++ {
				_, err = tb1.Insert(document.NewFieldBuffer().
					Add("a", types.NewIntegerValue(i)).
					Add("b", types.NewIntegerValue(i*10)),
				)
				assert.NoError(t, err)
				_, err = tb2.Insert(document.NewFieldBuffer().
					Add("a", types.NewIntegerValue(i)).
					Add("b", types.NewIntegerValue(i*10)),
				)
				assert.NoError(t, err)
			}

			err = catalog.CreateIndex(tx, &database.IndexInfo{
				IndexName: "t1a",
				TableName: "test1",
				Paths:     []document.Path{testutil.ParseDocumentPath(t, "a")},
			})
			assert.NoError(t, err)
			err = catalog.CreateIndex(tx, &database.IndexInfo{
				IndexName: "t2a",
				TableName: "test2",
				Paths:     []document.Path{testutil.ParseDocumentPath(t, "a")},
			})
			assert.NoError(t, err)

			return nil
		})

		clone := cloneCatalog(db.Catalog)

		updateCatalog(t, db, func(tx *database.Transaction, catalog *database.Catalog) error {
			err := catalog.ReIndexAll(tx)
			assert.NoError(t, err)
			idx, err := catalog.GetIndex(tx, "t1a")
			assert.NoError(t, err)

			var i int
			err = idx.AscendGreaterOrEqual([]types.Value{types.NewEmptyValue(types.DoubleValue)}, func(v, k []byte) error {
				var buf bytes.Buffer
				err = types.NewValueEncoder(&buf).Encode(
					types.NewArrayValue(
						document.NewValueBuffer(
							types.NewDoubleValue(float64(i)),
						),
					),
				)
				assert.NoError(t, err)
				enc := buf.Bytes()
				require.Equal(t, enc, v)
				i++
				return nil
			})
			require.Equal(t, 10, i)
			assert.NoError(t, err)

			idx, err = catalog.GetIndex(tx, "t2a")
			assert.NoError(t, err)

			i = 0
			err = idx.AscendGreaterOrEqual([]types.Value{types.NewEmptyValue(types.DoubleValue)}, func(v, k []byte) error {
				var buf bytes.Buffer
				err = types.NewValueEncoder(&buf).Encode(
					types.NewArrayValue(
						document.NewValueBuffer(
							types.NewDoubleValue(float64(i)),
						),
					),
				)
				assert.NoError(t, err)
				enc := buf.Bytes()
				require.Equal(t, enc, v)
				i++
				return nil
			})
			require.Equal(t, 10, i)
			assert.NoError(t, err)

			return errDontCommit
		})

		require.Equal(t, clone, db.Catalog)
	})
}

func TestReadOnlyTables(t *testing.T) {
	db, err := genji.Open(":memory:")
	assert.NoError(t, err)
	defer db.Close()

	res, err := db.Query(`
		CREATE TABLE foo (a int, b[3].c double unique);
		CREATE INDEX idx_foo_a ON foo(a);
		SELECT * FROM __genji_catalog
	`)
	assert.NoError(t, err)
	defer res.Close()

	var i int
	err = res.Iterate(func(d types.Document) error {
		switch i {
		case 0:
			testutil.RequireDocJSONEq(t, d, `{"name":"__genji_sequence", "sql":"CREATE TABLE __genji_sequence (name TEXT PRIMARY KEY, seq INTEGER)", "store_name":"X19nZW5qaV9zZXF1ZW5jZQ==", "type":"table"}`)
		case 1:
			testutil.RequireDocJSONEq(t, d, `{"name":"__genji_store_seq", "owner":{"table_name":"__genji_catalog"}, "sql":"CREATE SEQUENCE __genji_store_seq CACHE 16", "type":"sequence"}`)
		case 2:
			testutil.RequireDocJSONEq(t, d, `{"name":"foo", "docid_sequence_name":"foo_seq", "sql":"CREATE TABLE foo (a INTEGER, b[3].c DOUBLE UNIQUE)", "store_name":"AQ==", "type":"table"}`)
		case 3:
			testutil.RequireDocJSONEq(t, d, `{"name":"foo_b[3].c_idx", "owner":{"table_name":"foo", "path":"b[3].c"}, "sql":"CREATE UNIQUE INDEX `+"`foo_b[3].c_idx`"+` ON foo (b[3].c)", "store_name":"Ag==", "table_name":"foo", "type":"index"}`)
		case 4:
			testutil.RequireDocJSONEq(t, d, `{"name":"foo_seq", "owner":{"table_name":"foo"}, "sql":"CREATE SEQUENCE foo_seq CACHE 64", "type":"sequence"}`)
		case 5:
			testutil.RequireDocJSONEq(t, d, `{"name":"idx_foo_a", "sql":"CREATE INDEX idx_foo_a ON foo (a)", "store_name":"Aw==", "table_name":"foo", "type":"index"}`)
		default:
			t.Fatalf("count should be 5, got %d", i)
		}

		i++
		return nil
	})
	assert.NoError(t, err)
}

func TestCatalogCreateSequence(t *testing.T) {
	t.Run("Should create a sequence and add it to the schema and sequence tables", func(t *testing.T) {
		db, cleanup := testutil.NewTestDB(t)
		defer cleanup()

		updateCatalog(t, db, func(tx *database.Transaction, clog *database.Catalog) error {
			err := clog.CreateSequence(tx, &database.SequenceInfo{Name: "test1", IncrementBy: 1})
			if err != nil {
				return err
			}

			seq, err := clog.GetSequence("test1")
			assert.NoError(t, err)
			require.NotNil(t, seq)

			tb := db.Catalog.CatalogTable.Table(tx)
			key, err := tb.EncodeValue(types.NewTextValue("test1"))
			assert.NoError(t, err)

			_, err = tb.GetDocument(key)
			assert.NoError(t, err)

			tb, err = db.Catalog.GetTable(tx, database.SequenceTableName)
			assert.NoError(t, err)

			_, err = tb.GetDocument(key)
			assert.NoError(t, err)
			return nil
		})

		clone := cloneCatalog(db.Catalog)

		updateCatalog(t, db, func(tx *database.Transaction, catalog *database.Catalog) error {
			err := catalog.CreateSequence(tx, &database.SequenceInfo{Name: "test2", IncrementBy: 1})
			if err != nil {
				return err
			}
			seq, err := catalog.GetSequence("test2")
			assert.NoError(t, err)
			require.NotNil(t, seq)

			return errDontCommit
		})

		require.Equal(t, clone, db.Catalog)
	})

	t.Run("Should generate a sequence name if not provided", func(t *testing.T) {
		db, cleanup := testutil.NewTestDB(t)
		defer cleanup()

		updateCatalog(t, db, func(tx *database.Transaction, catalog *database.Catalog) error {
			for i := 0; i < 10; i++ {
				seqInfo := &database.SequenceInfo{IncrementBy: 1, Owner: database.Owner{
					TableName: "foo",
				}}
				err := catalog.CreateSequence(tx, seqInfo)
				if err != nil {
					return err
				}

				if i == 0 {
					require.Equal(t, "foo_seq", seqInfo.Name)
				} else {
					require.Equal(t, fmt.Sprintf("foo_seq%d", i), seqInfo.Name)
				}
			}

			return nil
		})
	})

	t.Run("Should fail if it already exists", func(t *testing.T) {
		db, cleanup := testutil.NewTestDB(t)
		defer cleanup()

		updateCatalog(t, db, func(tx *database.Transaction, catalog *database.Catalog) error {
			return catalog.CreateSequence(tx, &database.SequenceInfo{Name: "test"})
		})

		updateCatalog(t, db, func(tx *database.Transaction, catalog *database.Catalog) error {
			err := catalog.CreateSequence(tx, &database.SequenceInfo{Name: "test"})
			assert.ErrorIs(t, err, errs.AlreadyExistsError{Name: "test"})
			return nil
		})
	})
}
