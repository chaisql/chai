package database_test

import (
	"fmt"
	"math"
	"testing"

	"github.com/chaisql/chai"
	"github.com/chaisql/chai/internal/database"
	errs "github.com/chaisql/chai/internal/errors"
	"github.com/chaisql/chai/internal/expr"
	"github.com/chaisql/chai/internal/testutil"
	"github.com/chaisql/chai/internal/testutil/assert"
	"github.com/chaisql/chai/internal/tree"
	"github.com/chaisql/chai/internal/types"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func updateCatalog(t testing.TB, db *database.Database, fn func(tx *database.Transaction, catalog *database.CatalogWriter) error) {
	t.Helper()

	tx, err := db.Begin(true)
	assert.NoError(t, err)
	defer tx.Rollback()

	err = fn(tx, tx.CatalogWriter())
	if errors.Is(err, errDontCommit) {
		tx.Rollback()
		return
	}
	assert.NoError(t, err)

	err = tx.Commit()
	assert.NoError(t, err)
}

// TestCatalogTable tests all basic operations on tables:
// - CreateTable
// - GetTable
// - DropTable
// - RenameTable
// - AddColumnConstraint
func TestCatalogTable(t *testing.T) {
	t.Run("Get", func(t *testing.T) {
		db := testutil.NewTestDB(t)

		updateCatalog(t, db, func(tx *database.Transaction, catalog *database.CatalogWriter) error {
			return catalog.CreateTable(tx, "test", nil)
		})

		updateCatalog(t, db, func(tx *database.Transaction, catalog *database.CatalogWriter) error {
			table, err := catalog.GetTable(tx, "test")
			assert.NoError(t, err)
			require.Equal(t, "test", table.Info.TableName)

			// Getting a table that doesn't exist should fail.
			_, err = catalog.GetTable(tx, "unknown")
			if !errs.IsNotFoundError(err) {
				assert.ErrorIs(t, err, errs.NewNotFoundError("unknown"))
			}

			return nil
		})
	})

	t.Run("Drop", func(t *testing.T) {
		db := testutil.NewTestDB(t)

		updateCatalog(t, db, func(tx *database.Transaction, catalog *database.CatalogWriter) error {
			return catalog.CreateTable(tx, "test", nil)
		})

		clone := db.Catalog().Clone()

		updateCatalog(t, db, func(tx *database.Transaction, catalog *database.CatalogWriter) error {
			err := catalog.DropTable(tx, "test")
			assert.NoError(t, err)

			// Getting a table that has been dropped should fail.
			_, err = catalog.GetTable(tx, "test")
			if !errs.IsNotFoundError(err) {
				assert.ErrorIs(t, err, errs.NewNotFoundError("test"))
			}

			// Dropping a table that doesn't exist should fail.
			err = catalog.DropTable(tx, "test")
			if !errs.IsNotFoundError(err) {
				assert.ErrorIs(t, err, errs.NewNotFoundError("test"))
			}

			return errDontCommit
		})

		require.Equal(t, clone, db.Catalog())
	})

	t.Run("Rename", func(t *testing.T) {
		db := testutil.NewTestDB(t)

		ti := &database.TableInfo{
			ColumnConstraints: database.MustNewColumnConstraints(
				&database.ColumnConstraint{Column: "name", Type: types.TypeText, IsNotNull: true},
				&database.ColumnConstraint{Column: "age", Type: types.TypeInteger},
				&database.ColumnConstraint{Column: "gender", Type: types.TypeText},
				&database.ColumnConstraint{Column: "city", Type: types.TypeText},
			), TableConstraints: []*database.TableConstraint{
				{Columns: []string{"age"}, PrimaryKey: true},
			}}

		updateCatalog(t, db, func(tx *database.Transaction, catalog *database.CatalogWriter) error {
			err := catalog.CreateTable(tx, "foo", ti)
			assert.NoError(t, err)

			_, err = catalog.CreateIndex(tx, &database.IndexInfo{Columns: []string{"gender"}, IndexName: "idx_gender", Owner: database.Owner{TableName: "foo"}})
			assert.NoError(t, err)
			_, err = catalog.CreateIndex(tx, &database.IndexInfo{Columns: []string{"city"}, IndexName: "idx_city", Owner: database.Owner{TableName: "foo"}, Unique: true})
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

		clone := db.Catalog().Clone()

		updateCatalog(t, db, func(tx *database.Transaction, catalog *database.CatalogWriter) error {
			err := catalog.RenameTable(tx, "foo", "zoo")
			assert.NoError(t, err)

			// Getting the old table should return an error.
			_, err = catalog.GetTable(tx, "foo")
			if !errs.IsNotFoundError(err) {
				assert.ErrorIs(t, err, errs.NewNotFoundError("foo"))
			}

			tb, err := catalog.GetTable(tx, "zoo")
			assert.NoError(t, err)
			// The field constraints should be the same.

			require.Equal(t, ti.ColumnConstraints, tb.Info.ColumnConstraints)

			// Check that the indexes have been updated as well.
			idxs := catalog.ListIndexes(tb.Info.TableName)
			require.Len(t, idxs, 2)
			for _, name := range idxs {
				info, err := catalog.GetIndexInfo(name)
				assert.NoError(t, err)
				require.Equal(t, "zoo", info.Owner.TableName)
			}

			// Check that the sequences have been updated as well.
			seq, err := catalog.GetSequence("seq_foo")
			assert.NoError(t, err)
			require.Equal(t, "zoo", seq.Info.Owner.TableName)

			// Renaming a non existing table should return an error
			err = catalog.RenameTable(tx, "foo", "")
			if !errs.IsNotFoundError(err) {
				assert.ErrorIs(t, err, errs.NewNotFoundError("foo"))
			}

			return errDontCommit
		})

		require.Equal(t, clone, db.Catalog())
	})

	t.Run("Add column constraint", func(t *testing.T) {
		db := testutil.NewTestDB(t)

		ti := &database.TableInfo{ColumnConstraints: database.MustNewColumnConstraints(
			&database.ColumnConstraint{Column: "name", Type: types.TypeText, IsNotNull: true},
			&database.ColumnConstraint{Column: "age", Type: types.TypeInteger},
			&database.ColumnConstraint{Column: "gender", Type: types.TypeText},
			&database.ColumnConstraint{Column: "city", Type: types.TypeText},
		), TableConstraints: []*database.TableConstraint{
			{Columns: []string{"age"}, PrimaryKey: true},
		}}

		updateCatalog(t, db, func(tx *database.Transaction, catalog *database.CatalogWriter) error {
			return catalog.CreateTable(tx, "foo", ti)
		})

		clone := db.Catalog().Clone()

		updateCatalog(t, db, func(tx *database.Transaction, catalog *database.CatalogWriter) error {

			// Add field constraint
			fieldToAdd := database.ColumnConstraint{
				Column: "last_name", Type: types.TypeText,
			}
			// Add table constraint
			var tcs database.TableConstraints
			tcs = append(tcs, &database.TableConstraint{
				Check: expr.Constraint(testutil.ParseExpr(t, "last_name > first_name")),
			})
			err := catalog.AddColumnConstraint(tx, "foo", &fieldToAdd, tcs)
			assert.NoError(t, err)

			tb, err := catalog.GetTable(tx, "foo")
			assert.NoError(t, err)

			// The field constraints should not be the same.
			require.Contains(t, tb.Info.ColumnConstraints.Ordered, &fieldToAdd)
			require.Equal(t, expr.Constraint(testutil.ParseExpr(t, "last_name > first_name")), tb.Info.TableConstraints[1].Check)

			// Renaming a non existing table should return an error
			err = catalog.AddColumnConstraint(tx, "bar", &fieldToAdd, nil)
			if !errs.IsNotFoundError(err) {
				assert.ErrorIs(t, err, errs.NewNotFoundError("bar"))
			}

			// Adding a existing field should return an error
			err = catalog.AddColumnConstraint(tx, "foo", ti.ColumnConstraints.Ordered[0], nil)
			assert.Error(t, err)

			// Adding a second primary key should return an error
			err = catalog.AddColumnConstraint(tx, "foo", nil, database.TableConstraints{
				{Columns: []string{"age"}, PrimaryKey: true},
			})
			assert.Error(t, err)

			return errDontCommit
		})

		require.Equal(t, clone, db.Catalog())
	})
}

func TestCatalogCreateTable(t *testing.T) {
	t.Run("Same table name", func(t *testing.T) {
		db := testutil.NewTestDB(t)

		clone := db.Catalog().Clone()

		updateCatalog(t, db, func(tx *database.Transaction, catalog *database.CatalogWriter) error {
			err := catalog.CreateTable(tx, "test", nil)
			assert.NoError(t, err)

			// Creating a table that already exists should fail.
			err = catalog.CreateTable(tx, "test", nil)
			assert.ErrorIs(t, err, errs.AlreadyExistsError{Name: "test"})

			return errDontCommit
		})

		require.Equal(t, clone, db.Catalog())
	})

	t.Run("Create and rollback", func(t *testing.T) {
		db := testutil.NewTestDB(t)

		check := func() {
			updateCatalog(t, db, func(tx *database.Transaction, catalog *database.CatalogWriter) error {
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

		updateCatalog(t, db, func(tx *database.Transaction, catalog *database.CatalogWriter) error {
			return catalog.CreateTable(tx, "test", &database.TableInfo{
				ColumnConstraints: database.MustNewColumnConstraints(
					&database.ColumnConstraint{Column: "a", Type: types.TypeText},
				),
				TableConstraints: []*database.TableConstraint{
					{Columns: []string{"a"}, PrimaryKey: true},
				},
			})
		})

		clone := db.Catalog().Clone()

		updateCatalog(t, db, func(tx *database.Transaction, catalog *database.CatalogWriter) error {
			_, err := catalog.CreateIndex(tx, &database.IndexInfo{
				IndexName: "idx_a", Owner: database.Owner{TableName: "test"}, Columns: []string{"a"},
			})
			assert.NoError(t, err)
			idx, err := catalog.GetIndex(tx, "idx_a")
			assert.NoError(t, err)
			require.NotNil(t, idx)

			return errDontCommit
		})

		require.Equal(t, clone, db.Catalog())
	})

	t.Run("Should fail if it already exists", func(t *testing.T) {
		db := testutil.NewTestDB(t)

		updateCatalog(t, db, func(tx *database.Transaction, catalog *database.CatalogWriter) error {
			return catalog.CreateTable(tx, "test", &database.TableInfo{
				ColumnConstraints: database.MustNewColumnConstraints(
					&database.ColumnConstraint{Column: "foo", Type: types.TypeText},
				),
			})
		})

		updateCatalog(t, db, func(tx *database.Transaction, catalog *database.CatalogWriter) error {
			_, err := catalog.CreateIndex(tx, &database.IndexInfo{
				IndexName: "idxFoo", Owner: database.Owner{TableName: "test"}, Columns: []string{"foo"},
			})
			assert.NoError(t, err)

			_, err = catalog.CreateIndex(tx, &database.IndexInfo{
				IndexName: "idxFoo", Owner: database.Owner{TableName: "test"}, Columns: []string{"foo"},
			})
			assert.ErrorIs(t, err, errs.AlreadyExistsError{Name: "idxFoo"})
			return nil
		})
	})

	t.Run("Should fail if table doesn't exist", func(t *testing.T) {
		db := testutil.NewTestDB(t)
		updateCatalog(t, db, func(tx *database.Transaction, catalog *database.CatalogWriter) error {
			_, err := catalog.CreateIndex(tx, &database.IndexInfo{
				IndexName: "idxFoo", Owner: database.Owner{TableName: "test"}, Columns: []string{"foo"},
			})
			if !errs.IsNotFoundError(err) {
				assert.ErrorIs(t, err, errs.NewNotFoundError("test"))
			}

			return nil
		})
	})

	t.Run("Should generate a name if not provided", func(t *testing.T) {
		db := testutil.NewTestDB(t)

		updateCatalog(t, db, func(tx *database.Transaction, catalog *database.CatalogWriter) error {
			return catalog.CreateTable(tx, "test", &database.TableInfo{
				ColumnConstraints: database.MustNewColumnConstraints(
					&database.ColumnConstraint{Column: "foo", Type: types.TypeInteger},
				),
			})
		})

		updateCatalog(t, db, func(tx *database.Transaction, catalog *database.CatalogWriter) error {
			_, err := catalog.CreateIndex(tx, &database.IndexInfo{
				Owner: database.Owner{TableName: "test"}, Columns: []string{"foo"},
			})
			assert.NoError(t, err)

			_, err = catalog.GetIndex(tx, "test_foo_idx")
			assert.NoError(t, err)

			// create another one
			_, err = catalog.CreateIndex(tx, &database.IndexInfo{
				Owner: database.Owner{TableName: "test"}, Columns: []string{"foo"},
			})
			assert.NoError(t, err)

			_, err = catalog.GetIndex(tx, "test_foo_idx1")
			assert.NoError(t, err)
			return nil
		})
	})
}

func TestTxDropIndex(t *testing.T) {
	t.Run("Should drop an index", func(t *testing.T) {
		db := testutil.NewTestDB(t)

		updateCatalog(t, db, func(tx *database.Transaction, catalog *database.CatalogWriter) error {
			err := catalog.CreateTable(tx, "test", &database.TableInfo{
				ColumnConstraints: database.MustNewColumnConstraints(
					&database.ColumnConstraint{Column: "foo", Type: types.TypeText},
					&database.ColumnConstraint{Column: "bar", Type: types.TypeBoolean},
				),
			})
			assert.NoError(t, err)
			_, err = catalog.CreateIndex(tx, &database.IndexInfo{
				IndexName: "idxFoo", Owner: database.Owner{TableName: "test"}, Columns: []string{"foo"},
			})
			assert.NoError(t, err)
			_, err = catalog.CreateIndex(tx, &database.IndexInfo{
				IndexName: "idxBar", Owner: database.Owner{TableName: "test"}, Columns: []string{"bar"},
			})
			assert.NoError(t, err)
			return nil
		})

		clone := db.Catalog().Clone()
		updateCatalog(t, db, func(tx *database.Transaction, catalog *database.CatalogWriter) error {
			err := catalog.DropIndex(tx, "idxFoo")
			assert.NoError(t, err)

			_, err = catalog.GetIndex(tx, "idxFoo")
			assert.Error(t, err)

			_, err = catalog.GetIndex(tx, "idxBar")
			assert.NoError(t, err)

			// cf: https://github.com/chaisql/chai/issues/360
			_, err = catalog.GetTable(tx, "test")
			assert.NoError(t, err)

			return errDontCommit
		})

		require.Equal(t, clone, db.Catalog())
	})

	t.Run("Should fail if it doesn't exist", func(t *testing.T) {
		db := testutil.NewTestDB(t)

		updateCatalog(t, db, func(tx *database.Transaction, catalog *database.CatalogWriter) error {
			err := catalog.DropIndex(tx, "idxFoo")
			assert.ErrorIs(t, err, errs.NewNotFoundError("idxFoo"))
			return nil
		})
	})
}

func TestReadOnlyTables(t *testing.T) {
	db, err := chai.Open(":memory:")
	assert.NoError(t, err)
	defer db.Close()

	res, err := db.Query(`
		CREATE TABLE foo (a int, b double unique, c text);
		CREATE INDEX idx_foo_a ON foo(a, c);
		SELECT * FROM __chai_catalog
	`)
	assert.NoError(t, err)
	defer res.Close()

	var i int
	err = res.Iterate(func(r *chai.Row) error {
		switch i {
		case 0:
			testutil.RequireJSONEq(t, r, `{"name":"__chai_catalog", "namespace":1, "owner_table_name": null, "owner_table_columns": null, "rowid_sequence_name": null, "sql":"CREATE TABLE __chai_catalog (name TEXT NOT NULL, type TEXT NOT NULL, namespace BIGINT, sql TEXT, rowid_sequence_name TEXT, owner_table_name TEXT, owner_table_columns TEXT, CONSTRAINT __chai_catalog_pk PRIMARY KEY (name))", "type":"table"}`)
		case 1:
			testutil.RequireJSONEq(t, r, `{"name":"__chai_sequence", "namespace":2, "owner_table_name": null, "owner_table_columns":null, "rowid_sequence_name": null, "sql":"CREATE TABLE __chai_sequence (name TEXT NOT NULL, seq BIGINT, CONSTRAINT __chai_sequence_pk PRIMARY KEY (name))", "type":"table"}`)
		case 2:
			testutil.RequireJSONEq(t, r, `{"name":"__chai_store_seq", "namespace":null, "owner_table_name": "__chai_catalog", "owner_table_columns":null, "rowid_sequence_name": null, "sql":"CREATE SEQUENCE __chai_store_seq MAXVALUE 9223372036837998591 START WITH 10 CACHE 0", "type":"sequence"}`)
		case 3:
			testutil.RequireJSONEq(t, r, `{"name":"foo", "namespace":10, "owner_table_name": null, "owner_table_columns":null, "rowid_sequence_name":"foo_seq", "sql":"CREATE TABLE foo (a INTEGER, b DOUBLE, c TEXT, CONSTRAINT foo_b_unique UNIQUE (b))", "namespace":10, "type":"table"}`)
		case 4:
			testutil.RequireJSONEq(t, r, `{"name":"foo_b_idx", "namespace":11, "owner_table_name":"foo", "owner_table_columns": "b", "rowid_sequence_name": null, "sql":"CREATE UNIQUE INDEX foo_b_idx ON foo (b)", "type":"index"}`)
		case 5:
			testutil.RequireJSONEq(t, r, `{"name":"foo_seq", "namespace":null, "owner_table_name":"foo", "owner_table_columns":null, "rowid_sequence_name": null, "sql":"CREATE SEQUENCE foo_seq CACHE 64", "type":"sequence"}`)
		case 6:
			testutil.RequireJSONEq(t, r, `{"name":"idx_foo_a", "namespace":12, "owner_table_name":"foo", "owner_table_columns":null, "rowid_sequence_name": null, "sql":"CREATE INDEX idx_foo_a ON foo (a, c)", "type":"index", "owner_table_name":"foo"}`)
		default:
			t.Fatalf("count should be 6, got %d", i)
		}

		i++
		return nil
	})
	assert.NoError(t, err)
}

func TestCatalogCreateSequence(t *testing.T) {
	t.Run("Should create a sequence and add it to the schema and sequence tables", func(t *testing.T) {
		db := testutil.NewTestDB(t)

		updateCatalog(t, db, func(tx *database.Transaction, clog *database.CatalogWriter) error {
			err := clog.CreateSequence(tx, &database.SequenceInfo{Name: "test1", IncrementBy: 1})
			if err != nil {
				return err
			}

			seq, err := clog.GetSequence("test1")
			assert.NoError(t, err)
			require.NotNil(t, seq)

			tb := db.Catalog().CatalogTable.Table(tx)
			key := tree.NewKey(types.NewTextValue("test1"))

			_, err = tb.GetRow(key)
			assert.NoError(t, err)

			tb, err = db.Catalog().GetTable(tx, database.SequenceTableName)
			assert.NoError(t, err)

			_, err = tb.GetRow(key)
			assert.NoError(t, err)
			return nil
		})

		clone := db.Catalog().Clone()

		updateCatalog(t, db, func(tx *database.Transaction, catalog *database.CatalogWriter) error {
			err := catalog.CreateSequence(tx, &database.SequenceInfo{Name: "test2", IncrementBy: 1})
			if err != nil {
				return err
			}
			seq, err := catalog.GetSequence("test2")
			assert.NoError(t, err)
			require.NotNil(t, seq)

			return errDontCommit
		})

		require.Equal(t, clone, db.Catalog())
	})

	t.Run("Should generate a sequence name if not provided", func(t *testing.T) {
		db := testutil.NewTestDB(t)

		updateCatalog(t, db, func(tx *database.Transaction, catalog *database.CatalogWriter) error {
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

		updateCatalog(t, db, func(tx *database.Transaction, catalog *database.CatalogWriter) error {
			return catalog.CreateSequence(tx, &database.SequenceInfo{Name: "test"})
		})

		updateCatalog(t, db, func(tx *database.Transaction, catalog *database.CatalogWriter) error {
			err := catalog.CreateSequence(tx, &database.SequenceInfo{Name: "test"})
			assert.ErrorIs(t, err, errs.AlreadyExistsError{Name: "test"})
			return nil
		})
	})
}

func TestCatalogConcurrency(t *testing.T) {
	db, err := chai.Open(":memory:")
	assert.NoError(t, err)
	defer db.Close()

	// create a table
	err = db.Exec(`
		CREATE TABLE test (a int);
		CREATE INDEX idx_test_a ON test(a);
	`)
	assert.NoError(t, err)

	// start a transaction rt1
	rt1, err := db.Begin(false)
	assert.NoError(t, err)
	defer rt1.Rollback()

	// start a transaction wt2
	wt1, err := db.Begin(true)
	assert.NoError(t, err)
	defer wt1.Rollback()

	// update the catalog in wt2
	err = wt1.Exec(`
		CREATE TABLE test2 (a int);
		CREATE INDEX idx_test2_a ON test2(a);
		ALTER TABLE test ADD COLUMN b int;
	`)
	assert.NoError(t, err)

	// get the table in rt1: should not see the changes made by wt2
	row, err := rt1.QueryRow("SELECT COUNT(*) FROM __chai_catalog WHERE name LIKE '%test2%'")
	assert.NoError(t, err)
	var i int
	err = row.Scan(&i)
	assert.NoError(t, err)
	require.Equal(t, 0, i)

	// get the modified table in rt1: should not see the changes made by wt2
	row, err = rt1.QueryRow("SELECT sql FROM __chai_catalog WHERE name = 'test'")
	assert.NoError(t, err)
	var s string
	err = row.Scan(&s)
	assert.NoError(t, err)
	require.Equal(t, "CREATE TABLE test (a INTEGER)", s)

	// commit wt2
	err = wt1.Commit()
	assert.NoError(t, err)

	// get the table in rt1: should not see the changes made by wt2
	row, err = rt1.QueryRow("SELECT COUNT(*) FROM __chai_catalog WHERE name LIKE '%test2%'")
	assert.NoError(t, err)
	err = row.Scan(&i)
	assert.NoError(t, err)
	require.Equal(t, 0, i)

	// get the modified table in rt1: should not see the changes made by wt2
	row, err = rt1.QueryRow("SELECT sql FROM __chai_catalog WHERE name = 'test'")
	assert.NoError(t, err)
	err = row.Scan(&s)
	assert.NoError(t, err)
	require.Equal(t, "CREATE TABLE test (a INTEGER)", s)
}
