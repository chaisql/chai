package database_test

import (
	"fmt"
	"math"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/genjidb/genji"
	"github.com/genjidb/genji/document"
	errs "github.com/genjidb/genji/errors"
	"github.com/genjidb/genji/internal/database"
	"github.com/genjidb/genji/internal/expr"
	"github.com/genjidb/genji/internal/testutil"
	"github.com/genjidb/genji/internal/testutil/assert"
	"github.com/genjidb/genji/internal/tree"
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
	clone.Locks = c.Locks
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
		db := testutil.NewTestDB(t)

		updateCatalog(t, db, func(tx *database.Transaction, catalog *database.Catalog) error {
			return catalog.CreateTable(tx, "test", nil)
		})

		updateCatalog(t, db, func(tx *database.Transaction, catalog *database.Catalog) error {
			table, err := catalog.GetTable(tx, "test")
			assert.NoError(t, err)
			require.Equal(t, "test", table.Info.TableName)

			// Getting a table that doesn't exist should fail.
			_, err = catalog.GetTable(tx, "unknown")
			if !errors.Is(err, errs.NotFoundError{}) {
				assert.ErrorIs(t, err, errs.NotFoundError{Name: "unknown"})
			}

			return nil
		})
	})

	t.Run("Drop", func(t *testing.T) {
		db := testutil.NewTestDB(t)

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
		db := testutil.NewTestDB(t)

		ti := &database.TableInfo{
			FieldConstraints: database.MustNewFieldConstraints(
				&database.FieldConstraint{Field: "name", Type: types.TextValue, IsNotNull: true},
				&database.FieldConstraint{Field: "age", Type: types.IntegerValue},
				&database.FieldConstraint{Field: "gender", Type: types.TextValue},
				&database.FieldConstraint{Field: "city", Type: types.TextValue},
			), TableConstraints: []*database.TableConstraint{
				{Paths: []document.Path{testutil.ParseDocumentPath(t, "age")}, PrimaryKey: true},
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
			idxs := catalog.ListIndexes(tb.Info.TableName)
			require.Len(t, idxs, 2)
			for _, name := range idxs {
				info, err := catalog.GetIndexInfo(name)
				assert.NoError(t, err)
				require.Equal(t, "zoo", info.TableName)
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
		db := testutil.NewTestDB(t)

		ti := &database.TableInfo{FieldConstraints: database.MustNewFieldConstraints(
			&database.FieldConstraint{Field: "name", Type: types.TextValue, IsNotNull: true},
			&database.FieldConstraint{Field: "age", Type: types.IntegerValue},
			&database.FieldConstraint{Field: "gender", Type: types.TextValue},
			&database.FieldConstraint{Field: "city", Type: types.TextValue},
		), TableConstraints: []*database.TableConstraint{
			{Paths: []document.Path{testutil.ParseDocumentPath(t, "age")}, PrimaryKey: true},
		}}

		updateCatalog(t, db, func(tx *database.Transaction, catalog *database.Catalog) error {
			return catalog.CreateTable(tx, "foo", ti)
		})

		clone := cloneCatalog(db.Catalog)

		updateCatalog(t, db, func(tx *database.Transaction, catalog *database.Catalog) error {

			// Add field constraint
			fieldToAdd := database.FieldConstraint{
				Field: "last_name", Type: types.TextValue,
			}
			// Add table constraint
			var tcs database.TableConstraints
			tcs = append(tcs, &database.TableConstraint{
				Check: expr.Constraint(testutil.ParseExpr(t, "last_name > first_name")),
			})
			err := catalog.AddFieldConstraint(tx, "foo", &fieldToAdd, tcs)
			assert.NoError(t, err)

			tb, err := catalog.GetTable(tx, "foo")
			assert.NoError(t, err)

			// The field constraints should not be the same.
			require.Contains(t, tb.Info.FieldConstraints.Ordered, &fieldToAdd)
			require.Equal(t, expr.Constraint(testutil.ParseExpr(t, "last_name > first_name")), tb.Info.TableConstraints[1].Check)

			// Renaming a non existing table should return an error
			err = catalog.AddFieldConstraint(tx, "bar", &fieldToAdd, nil)
			if !errors.Is(err, errs.NotFoundError{}) {
				assert.ErrorIs(t, err, errs.NotFoundError{Name: "bar"})
			}

			// Adding a existing field should return an error
			err = catalog.AddFieldConstraint(tx, "foo", ti.FieldConstraints.Ordered[0], nil)
			assert.Error(t, err)

			// Adding a second primary key should return an error
			err = catalog.AddFieldConstraint(tx, "foo", nil, database.TableConstraints{
				{Paths: []document.Path{testutil.ParseDocumentPath(t, "age")}, PrimaryKey: true},
			})
			assert.Error(t, err)

			return errDontCommit
		})

		require.Equal(t, clone, db.Catalog)
	})
}

func TestCatalogCreateTable(t *testing.T) {
	t.Run("Same table name", func(t *testing.T) {
		db := testutil.NewTestDB(t)

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
		db := testutil.NewTestDB(t)

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
}

func TestCatalogCreateIndex(t *testing.T) {
	t.Run("Should create an index, and return it", func(t *testing.T) {
		db := testutil.NewTestDB(t)

		updateCatalog(t, db, func(tx *database.Transaction, catalog *database.Catalog) error {
			return catalog.CreateTable(tx, "test", &database.TableInfo{
				TableConstraints: []*database.TableConstraint{
					{Paths: []document.Path{testutil.ParseDocumentPath(t, "a")}, PrimaryKey: true},
				},
			})
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

			return errDontCommit
		})

		require.Equal(t, clone, db.Catalog)
	})

	t.Run("Should fail if it already exists", func(t *testing.T) {
		db := testutil.NewTestDB(t)

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
		db := testutil.NewTestDB(t)
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
		db := testutil.NewTestDB(t)

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
		db := testutil.NewTestDB(t)

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
		db := testutil.NewTestDB(t)

		updateCatalog(t, db, func(tx *database.Transaction, catalog *database.Catalog) error {
			err := catalog.DropIndex(tx, "idxFoo")
			assert.ErrorIs(t, err, errs.NotFoundError{Name: "idxFoo"})
			return nil
		})
	})
}

func TestReadOnlyTables(t *testing.T) {
	db, err := genji.Open(":memory:")
	assert.NoError(t, err)
	defer db.Close()

	res, err := db.Query(`
		CREATE TABLE foo (a int, b (c double unique));
		CREATE INDEX idx_foo_a ON foo(a);
		SELECT * FROM __genji_catalog
	`)
	assert.NoError(t, err)
	defer res.Close()

	var i int
	err = res.Iterate(func(d types.Document) error {
		switch i {
		case 0:
			testutil.RequireDocJSONEq(t, d, `{"name":"__genji_sequence", "sql":"CREATE TABLE __genji_sequence (name TEXT, seq INTEGER, CONSTRAINT __genji_sequence_pk PRIMARY KEY (name))", "namespace":2, "type":"table"}`)
		case 1:
			testutil.RequireDocJSONEq(t, d, `{"name":"__genji_store_seq", "owner":{"table_name":"__genji_catalog"}, "sql":"CREATE SEQUENCE __genji_store_seq MAXVALUE 4294967295 START WITH 101 CACHE 0", "type":"sequence"}`)
		case 2:
			testutil.RequireDocJSONEq(t, d, `{"name":"foo", "docid_sequence_name":"foo_seq", "sql":"CREATE TABLE foo (a INTEGER, b (c DOUBLE), CONSTRAINT \"foo_b.c_unique\" UNIQUE (b.c))", "namespace":101, "type":"table"}`)
		case 3:
			testutil.RequireDocJSONEq(t, d, `{"name":"foo_b.c_idx", "owner":{"table_name":"foo", "paths":["b.c"]}, "sql":"CREATE UNIQUE INDEX `+"`foo_b.c_idx`"+` ON foo (b.c)", "namespace":102, "table_name":"foo", "type":"index"}`)
		case 4:
			testutil.RequireDocJSONEq(t, d, `{"name":"foo_seq", "owner":{"table_name":"foo"}, "sql":"CREATE SEQUENCE foo_seq CACHE 64", "type":"sequence"}`)
		case 5:
			testutil.RequireDocJSONEq(t, d, `{"name":"idx_foo_a", "sql":"CREATE INDEX idx_foo_a ON foo (a)", "namespace":103, "table_name":"foo", "type":"index"}`)
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
		db := testutil.NewTestDB(t)

		updateCatalog(t, db, func(tx *database.Transaction, clog *database.Catalog) error {
			err := clog.CreateSequence(tx, &database.SequenceInfo{Name: "test1", IncrementBy: 1})
			if err != nil {
				return err
			}

			seq, err := clog.GetSequence("test1")
			assert.NoError(t, err)
			require.NotNil(t, seq)

			tb := db.Catalog.CatalogTable.Table(tx)
			key, err := tree.NewKey(types.NewTextValue("test1"))
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
		db := testutil.NewTestDB(t)

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
		db := testutil.NewTestDB(t)

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
