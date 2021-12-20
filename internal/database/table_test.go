package database_test

import (
	"fmt"
	"testing"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/engine/memoryengine"
	errs "github.com/genjidb/genji/errors"
	"github.com/genjidb/genji/internal/database"
	"github.com/genjidb/genji/internal/errors"
	"github.com/genjidb/genji/internal/expr"
	"github.com/genjidb/genji/internal/query/statement"
	"github.com/genjidb/genji/internal/testutil"
	"github.com/genjidb/genji/internal/testutil/assert"
	"github.com/genjidb/genji/internal/tree"
	"github.com/genjidb/genji/types"
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
	db, tx, fn := testutil.NewTestTx(t)

	return createTable(t, tx, db.Catalog, database.TableInfo{TableName: "test"}), fn
}

func createTable(t testing.TB, tx *database.Transaction, catalog *database.Catalog, info database.TableInfo) *database.Table {
	stmt := statement.CreateTableStmt{Info: info}

	res, err := stmt.Run(&statement.Context{
		Catalog: catalog,
		Tx:      tx,
	})
	assert.NoError(t, err)
	res.Close()

	tb, err := catalog.GetTable(tx, stmt.Info.TableName)
	assert.NoError(t, err)

	return tb
}

func createTableIfNotExists(t testing.TB, tx *database.Transaction, catalog *database.Catalog, info database.TableInfo) *database.Table {
	stmt := statement.CreateTableStmt{Info: info, IfNotExists: true}

	res, err := stmt.Run(&statement.Context{
		Catalog: catalog,
		Tx:      tx,
	})
	assert.NoError(t, err)
	res.Close()

	tb, err := catalog.GetTable(tx, stmt.Info.TableName)
	assert.NoError(t, err)

	return tb
}

func newDocument() *document.FieldBuffer {
	return document.NewFieldBuffer().
		Add("fielda", types.NewTextValue("a")).
		Add("fieldb", types.NewTextValue("b"))
}

// TestTableGetDocument verifies GetDocument behaviour.
func TestTableGetDocument(t *testing.T) {
	t.Run("Should fail if not found", func(t *testing.T) {
		tb, cleanup := newTestTable(t)
		defer cleanup()

		r, err := tb.GetDocument([]byte("id"))
		assert.ErrorIs(t, err, errs.ErrDocumentNotFound)
		require.Nil(t, r)
	})

	t.Run("Should return the right document", func(t *testing.T) {
		tb, cleanup := newTestTable(t)
		defer cleanup()

		// create two documents, one with an additional field
		doc1 := newDocument()
		vc := types.NewDoubleValue(40)
		doc1.Add("fieldc", vc)
		doc2 := newDocument()

		key, _, err := tb.Insert(doc1)
		assert.NoError(t, err)
		_, _, err = tb.Insert(doc2)
		assert.NoError(t, err)

		// fetch doc1 and make sure it returns the right one
		res, err := tb.GetDocument(key)
		assert.NoError(t, err)
		fc, err := res.GetByField("fieldc")
		assert.NoError(t, err)
		ok, err := types.IsEqual(vc, fc)
		assert.NoError(t, err)
		require.True(t, ok)
	})
}

// TestTableInsert verifies Insert behaviour.
func TestTableInsert(t *testing.T) {
	t.Run("Should generate a key by default", func(t *testing.T) {
		tb, cleanup := newTestTable(t)
		defer cleanup()

		doc := newDocument()
		key1, _, err := tb.Insert(testutil.CloneDocument(t, doc))
		assert.NoError(t, err)
		require.NotEmpty(t, key1)

		key2, _, err := tb.Insert(testutil.CloneDocument(t, doc))
		assert.NoError(t, err)
		require.NotEmpty(t, key2)

		require.NotEqual(t, key1, key2)
	})

	t.Run("Should generate the right docid on existing databases", func(t *testing.T) {
		ng := memoryengine.NewEngine()

		db, cleanup := testutil.NewTestDBWithEngine(t, ng)
		defer cleanup()

		insertDoc := func(db *database.Database) (rawKey tree.Key) {
			update(t, db, func(tx *database.Transaction) error {
				// create table if not exists
				tb := createTableIfNotExists(t, tx, db.Catalog, database.TableInfo{TableName: "test"})

				doc := newDocument()
				key, _, err := tb.Insert(doc)
				assert.NoError(t, err)
				require.NotEmpty(t, key)
				rawKey = key
				return nil
			})
			return
		}

		key1 := insertDoc(db)

		err := db.Close()
		assert.NoError(t, err)

		ng.Closed = false

		// create a new database object
		db, cleanup = testutil.NewTestDBWithEngine(t, ng)
		defer cleanup()

		assert.NoError(t, err)

		key2 := insertDoc(db)

		vs, err := key1.Decode()
		assert.NoError(t, err)
		a := vs[0].V().(int64)

		vs, err = key2.Decode()
		assert.NoError(t, err)
		b := vs[0].V().(int64)

		require.Equal(t, int64(a+1), int64(b))
	})

	t.Run("Should use the right field if primary key is specified", func(t *testing.T) {
		db, tx, cleanup := newTestTx(t)
		defer cleanup()

		err := db.Catalog.CreateTable(tx, "test", &database.TableInfo{
			FieldConstraints: []*database.FieldConstraint{
				{Path: testutil.ParseDocumentPath(t, "foo.a[1]"), Type: types.IntegerValue},
			},
			TableConstraints: []*database.TableConstraint{
				{Paths: testutil.ParseDocumentPaths(t, "foo.a[1]"), PrimaryKey: true},
			},
		})
		assert.NoError(t, err)
		tb, err := db.Catalog.GetTable(tx, "test")
		assert.NoError(t, err)

		var doc document.FieldBuffer
		err = doc.UnmarshalJSON([]byte(`{"foo": {"a": [0, 10]}}`))
		assert.NoError(t, err)

		// insert
		key, _, err := tb.Insert(doc)
		assert.NoError(t, err)
		want, err := tree.NewKey(types.NewIntegerValue(10))
		assert.NoError(t, err)

		require.Equal(t, want, key)

		// make sure the document is fetchable using the returned key
		_, err = tb.GetDocument(key)
		assert.NoError(t, err)

		// insert again
		_, _, err = tb.Insert(doc)
		require.EqualError(t, err, "PRIMARY KEY constraint error: [foo.a[1]]")
	})

	t.Run("Should convert values into the right types if there are constraints", func(t *testing.T) {
		db, tx, cleanup := newTestTx(t)
		defer cleanup()

		tb := createTable(t, tx, db.Catalog, database.TableInfo{
			TableName: "test",
			FieldConstraints: []*database.FieldConstraint{
				{Path: testutil.ParseDocumentPath(t, "foo"), Type: types.ArrayValue},
				{Path: testutil.ParseDocumentPath(t, "foo[0]"), Type: types.IntegerValue},
			},
		})

		var doc document.FieldBuffer
		err := doc.UnmarshalJSON([]byte(`{"foo": [100]}`))
		assert.NoError(t, err)

		// insert
		key, _, err := tb.Insert(doc)
		assert.NoError(t, err)

		d, err := tb.GetDocument(key)
		assert.NoError(t, err)

		v, err := testutil.ParseDocumentPath(t, "foo[0]").GetValueFromDocument(d)
		assert.NoError(t, err)
		ok, err := types.IsEqual(types.NewIntegerValue(100), v)
		assert.NoError(t, err)
		require.True(t, ok)
	})

	t.Run("Should fail if Pk not found in document or empty", func(t *testing.T) {
		db, tx, cleanup := newTestTx(t)
		defer cleanup()

		err := db.Catalog.CreateTable(tx, "test", &database.TableInfo{
			FieldConstraints: []*database.FieldConstraint{
				{Path: testutil.ParseDocumentPath(t, "foo"), Type: types.IntegerValue},
			},
			TableConstraints: []*database.TableConstraint{
				{Paths: testutil.ParseDocumentPaths(t, "foo"), PrimaryKey: true},
			},
		})
		assert.NoError(t, err)
		tb, err := db.Catalog.GetTable(tx, "test")
		assert.NoError(t, err)

		tests := [][]byte{
			nil,
			{},
			[]byte(nil),
		}

		for _, test := range tests {
			t.Run(fmt.Sprintf("%#v", test), func(t *testing.T) {
				doc := document.NewFieldBuffer().
					Add("foo", types.NewBlobValue(test))

				_, err = tb.Info.ValidateDocument(tx, doc)
				assert.Error(t, err)
			})
		}
	})

	t.Run("Should convert the fields if FieldsConstraints are specified", func(t *testing.T) {
		db, tx, cleanup := newTestTx(t)
		defer cleanup()

		tb := createTable(t, tx, db.Catalog, database.TableInfo{
			TableName: "test",
			FieldConstraints: []*database.FieldConstraint{
				{Path: testutil.ParseDocumentPath(t, "foo"), Type: types.DocumentValue, IsInferred: true, InferredBy: []document.Path{testutil.ParseDocumentPath(t, "foo.bar")}},
				{Path: testutil.ParseDocumentPath(t, "foo.bar"), Type: types.IntegerValue, IsInferred: true, InferredBy: []document.Path{testutil.ParseDocumentPath(t, "foo")}},
			},
		})

		doc := document.NewFieldBuffer().
			Add("foo", types.NewDocumentValue(
				document.NewFieldBuffer().Add("bar", types.NewDoubleValue(10)),
			)).
			Add("bar", types.NewDoubleValue(10)).
			Add("baz", types.NewTextValue("baz")).
			Add("bat", types.NewIntegerValue(20))

		// insert
		fb, err := tb.Info.ValidateDocument(tx, doc)
		assert.NoError(t, err)
		key, _, err := tb.Insert(fb)
		assert.NoError(t, err)

		// make sure the fields have been converted to the right types
		d, err := tb.GetDocument(key)
		assert.NoError(t, err)
		v, err := d.GetByField("foo")
		assert.NoError(t, err)
		v, err = v.V().(types.Document).GetByField("bar")
		assert.NoError(t, err)
		ok, err := types.IsEqual(types.NewIntegerValue(10), v)
		assert.NoError(t, err)
		require.True(t, ok)
		v, err = d.GetByField("bar")
		assert.NoError(t, err)
		ok, err = types.IsEqual(types.NewDoubleValue(10), v)
		assert.NoError(t, err)
		require.True(t, ok)
		v, err = d.GetByField("baz")
		assert.NoError(t, err)
		ok, err = types.IsEqual(types.NewTextValue("baz"), v)
		assert.NoError(t, err)
		require.True(t, ok)
		v, err = d.GetByField("bat")
		assert.NoError(t, err)
		ok, err = types.IsEqual(types.NewDoubleValue(20), v)
		assert.NoError(t, err)
		require.True(t, ok)
	})

	t.Run("Should fail if the fields cannot be converted to specified field constraints", func(t *testing.T) {
		db, tx, cleanup := newTestTx(t)
		defer cleanup()

		err := db.Catalog.CreateTable(tx, "test", &database.TableInfo{
			FieldConstraints: []*database.FieldConstraint{
				{Path: testutil.ParseDocumentPath(t, "foo"), Type: types.DoubleValue},
			},
		})
		assert.NoError(t, err)
		tb, err := db.Catalog.GetTable(tx, "test")
		assert.NoError(t, err)

		doc := document.NewFieldBuffer().
			Add("foo", types.NewArrayValue(document.NewValueBuffer(types.NewIntegerValue(1))))

		// insert
		_, _, err = tb.Insert(doc)
		assert.Error(t, err)
	})

	t.Run("Should fail if there is a not null field constraint on a document field and the field is null or missing", func(t *testing.T) {
		db, tx, cleanup := newTestTx(t)
		defer cleanup()

		// no enforced type, not null
		tb1 := createTable(t, tx, db.Catalog, database.TableInfo{
			TableName: "test1",
			FieldConstraints: []*database.FieldConstraint{
				{Path: testutil.ParseDocumentPath(t, "foo"), IsNotNull: true},
			},
		})

		// enforced type, not null
		tb2 := createTable(t, tx, db.Catalog, database.TableInfo{
			TableName: "test2",
			FieldConstraints: []*database.FieldConstraint{
				{Path: testutil.ParseDocumentPath(t, "foo"), Type: types.IntegerValue, IsNotNull: true},
			},
		})

		// insert with empty foo field should fail
		_, err := tb1.Info.ValidateDocument(tx, document.NewFieldBuffer().
			Add("bar", types.NewDoubleValue(1)))
		assert.Error(t, err)

		// insert with null foo field should fail
		_, err = tb1.Info.ValidateDocument(tx, document.NewFieldBuffer().
			Add("foo", types.NewNullValue()))
		assert.Error(t, err)

		// otherwise it should work
		_, err = tb1.Info.ValidateDocument(tx, document.NewFieldBuffer().
			Add("foo", types.NewDoubleValue(1)))
		assert.NoError(t, err)

		// insert with empty foo field should fail
		_, err = tb2.Info.ValidateDocument(tx, document.NewFieldBuffer().
			Add("bar", types.NewDoubleValue(1)))
		assert.Error(t, err)

		// insert with null foo field should fail
		_, err = tb2.Info.ValidateDocument(tx, document.NewFieldBuffer().
			Add("foo", types.NewNullValue()))
		assert.Error(t, err)

		// otherwise it should work
		_, err = tb2.Info.ValidateDocument(tx, document.NewFieldBuffer().
			Add("foo", types.NewDoubleValue(1)))
		assert.NoError(t, err)
	})

	t.Run("Shouldn't fail if there is a not null field and default constraint on a document field and the field is null or missing", func(t *testing.T) {
		db, tx, cleanup := newTestTx(t)
		defer cleanup()

		// no enforced type, not null
		tb1 := createTable(t, tx, db.Catalog, database.TableInfo{
			TableName: "test1",
			FieldConstraints: []*database.FieldConstraint{
				{Path: testutil.ParseDocumentPath(t, "foo"), IsNotNull: true, DefaultValue: expr.Constraint(testutil.IntegerValue(42))},
			},
		})

		// enforced type, not null
		tb2 := createTable(t, tx, db.Catalog, database.TableInfo{
			TableName: "test2",
			FieldConstraints: []*database.FieldConstraint{
				{Path: testutil.ParseDocumentPath(t, "foo"), Type: types.IntegerValue, IsNotNull: true, DefaultValue: expr.Constraint(testutil.IntegerValue(42))},
			},
		})

		// insert with empty foo field shouldn't fail
		fb, err := tb1.Info.ValidateDocument(tx, document.NewFieldBuffer().
			Add("bar", types.NewDoubleValue(1)))
		assert.NoError(t, err)

		key, _, err := tb1.Insert(fb)
		assert.NoError(t, err)

		d, err := tb1.GetDocument(key)
		assert.NoError(t, err)
		v, err := d.GetByField("foo")
		assert.NoError(t, err)
		require.Equal(t, v.V().(float64), float64(42))

		// insert with explicit null foo field should fail
		_, err = tb1.Info.ValidateDocument(tx, document.NewFieldBuffer().
			Add("foo", types.NewNullValue()))
		assert.Error(t, err)

		// otherwise it should work
		_, err = tb1.Info.ValidateDocument(tx, document.NewFieldBuffer().
			Add("foo", types.NewIntegerValue(1)))
		assert.NoError(t, err)

		// insert with empty foo field shouldn't fail
		fb, err = tb2.Info.ValidateDocument(tx, document.NewFieldBuffer().
			Add("bar", types.NewIntegerValue(1)))
		assert.NoError(t, err)
		key, _, err = tb2.Insert(fb)
		assert.NoError(t, err)

		d, err = tb2.GetDocument(key)
		assert.NoError(t, err)
		v, err = d.GetByField("foo")
		assert.NoError(t, err)
		require.Equal(t, v.V().(int64), int64(42))

		// insert with explicit null foo field should fail
		_, err = tb2.Info.ValidateDocument(tx, document.NewFieldBuffer().
			Add("foo", types.NewNullValue()))
		assert.Error(t, err)

		// otherwise it should work
		_, err = tb2.Info.ValidateDocument(tx, document.NewFieldBuffer().
			Add("foo", types.NewDoubleValue(1)))
		assert.NoError(t, err)
	})

	t.Run("Should fail if there is a not null field constraint on an array value and the value is null", func(t *testing.T) {
		db, tx, cleanup := newTestTx(t)
		defer cleanup()

		tb := createTable(t, tx, db.Catalog, database.TableInfo{
			TableName: "test1",
			FieldConstraints: []*database.FieldConstraint{
				{Path: testutil.ParseDocumentPath(t, "foo[1]"), IsNotNull: true},
			},
		})

		// insert table with only one value
		_, err := tb.Info.ValidateDocument(tx, document.NewFieldBuffer().
			Add("foo", types.NewArrayValue(document.NewValueBuffer().Append(types.NewIntegerValue(1)))))
		assert.Error(t, err)
		_, err = tb.Info.ValidateDocument(tx, document.NewFieldBuffer().
			Add("foo", types.NewArrayValue(document.NewValueBuffer().
				Append(types.NewIntegerValue(1)).Append(types.NewIntegerValue(2)))))
		assert.NoError(t, err)
	})

	t.Run("Should fail if the pk is duplicated", func(t *testing.T) {
		db, tx, cleanup := newTestTx(t)
		defer cleanup()

		tb := createTable(t, tx, db.Catalog, database.TableInfo{
			TableName: "test",
			FieldConstraints: []*database.FieldConstraint{
				{Path: testutil.ParseDocumentPath(t, "foo"), IsNotNull: true},
			},
			TableConstraints: []*database.TableConstraint{
				{Paths: testutil.ParseDocumentPaths(t, "foo"), PrimaryKey: true},
			},
		})

		doc := document.NewFieldBuffer().
			Add("foo", types.NewIntegerValue(10))

		// insert first
		_, _, err := tb.Insert(doc)
		assert.NoError(t, err)

		// insert again, should fail
		_, _, err = tb.Insert(doc)
		require.EqualError(t, err, "PRIMARY KEY constraint error: [foo]")
	})
}

// TestTableDelete verifies Delete behaviour.
func TestTableDelete(t *testing.T) {
	t.Run("Should fail if not found", func(t *testing.T) {
		tb, cleanup := newTestTable(t)
		defer cleanup()

		err := tb.Delete([]byte("id"))
		assert.ErrorIs(t, err, errs.ErrDocumentNotFound)
	})

	t.Run("Should delete the right document", func(t *testing.T) {
		tb, cleanup := newTestTable(t)
		defer cleanup()

		// create two documents, one with an additional field
		doc1 := newDocument()
		doc1.Add("fieldc", types.NewIntegerValue(40))
		doc2 := newDocument()

		key1, _, err := tb.Insert(testutil.CloneDocument(t, doc1))
		assert.NoError(t, err)
		key2, _, err := tb.Insert(testutil.CloneDocument(t, doc2))
		assert.NoError(t, err)

		// delete the document
		err = tb.Delete([]byte(key1))
		assert.NoError(t, err)

		// try again, should fail
		err = tb.Delete([]byte(key1))
		assert.ErrorIs(t, err, errs.ErrDocumentNotFound)

		// make sure it didn't also delete the other one
		res, err := tb.GetDocument(key2)
		assert.NoError(t, err)
		_, err = res.GetByField("fieldc")
		assert.Error(t, err)
	})
}

// TestTableReplace verifies Replace behaviour.
func TestTableReplace(t *testing.T) {
	t.Run("Should fail if not found", func(t *testing.T) {
		tb, cleanup := newTestTable(t)
		defer cleanup()

		_, err := tb.Replace([]byte("id"), newDocument())
		assert.ErrorIs(t, err, errs.ErrDocumentNotFound)
	})

	t.Run("Should replace the right document", func(t *testing.T) {
		tb, cleanup := newTestTable(t)
		defer cleanup()

		// create two different documents
		doc1 := newDocument()
		doc2 := document.NewFieldBuffer().
			Add("fielda", types.NewTextValue("c")).
			Add("fieldb", types.NewTextValue("d"))

		key1, _, err := tb.Insert(doc1)
		assert.NoError(t, err)
		key2, _, err := tb.Insert(doc2)
		assert.NoError(t, err)

		// create a third document
		doc3 := document.NewFieldBuffer().
			Add("fielda", types.NewTextValue("e")).
			Add("fieldb", types.NewTextValue("f"))

		// replace doc1 with doc3
		d3, err := tb.Replace(key1, doc3)
		assert.NoError(t, err)

		// make sure it replaced it correctly
		res, err := tb.GetDocument(key1)
		assert.NoError(t, err)
		f, err := res.GetByField("fielda")
		assert.NoError(t, err)
		require.Equal(t, "e", f.V().(string))

		testutil.RequireDocEqual(t, d3, res)

		// make sure it didn't also replace the other one
		res, err = tb.GetDocument(key2)
		assert.NoError(t, err)
		f, err = res.GetByField("fielda")
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

		// create two documents
		doc1 := newDocument()
		doc2 := newDocument()

		_, _, err := tb.Insert(doc1)
		assert.NoError(t, err)
		_, _, err = tb.Insert(doc2)
		assert.NoError(t, err)

		err = tb.Truncate()
		assert.NoError(t, err)

		err = tb.IterateOnRange(nil, false, func(key tree.Key, _ types.Document) error {
			return errors.New("should not iterate")
		})

		assert.NoError(t, err)
	})
}

// BenchmarkTableInsert benchmarks the Insert method with 1, 10, 1000 and 10000 successive insertions.
func BenchmarkTableInsert(b *testing.B) {
	for size := 1; size <= 10000; size *= 10 {
		b.Run(fmt.Sprintf("%.05d", size), func(b *testing.B) {
			var fb document.FieldBuffer

			for i := int64(0); i < 10; i++ {
				fb.Add(fmt.Sprintf("name-%d", i), types.NewIntegerValue(i))
			}

			b.ResetTimer()
			b.StopTimer()
			for i := 0; i < b.N; i++ {
				tb, cleanup := newTestTable(b)

				b.StartTimer()
				for j := 0; j < size; j++ {
					tb.Insert(&fb)
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

			var fb document.FieldBuffer

			for i := int64(0); i < 10; i++ {
				fb.Add(fmt.Sprintf("name-%d", i), types.NewIntegerValue(i))
			}

			for i := 0; i < size; i++ {
				_, _, err := tb.Insert(&fb)
				assert.NoError(b, err)
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				tb.IterateOnRange(nil, false, func(tree.Key, types.Document) error {
					return nil
				})
			}
			b.StopTimer()
		})
	}
}
