// Package enginetest defines a list of tests that can be used to test
// a complete or partial engine implementation.
package enginetest

import (
	"testing"

	"github.com/asdine/genji/engine"
	"github.com/asdine/genji/field"
	"github.com/asdine/genji/index"
	"github.com/asdine/genji/record"
	"github.com/asdine/genji/table"
	"github.com/stretchr/testify/require"
)

// Builder is a function that can create an engine on demand and that provides
// a function to cleanup up and remove any created state.
// Tests will use the builder like this:
//     ng, cleanup := builder()
//     defer cleanup()
//     ...
type Builder func() (engine.Engine, func())

// TestSuite tests an entire engine, transaction and related types
// needed to implement a Genji engine.
func TestSuite(t *testing.T, builder Builder) {
	tests := []struct {
		name string
		test func(*testing.T, Builder)
	}{
		{"Engine", TestEngine},
		{"Transaction/Commit-Rollback", TestTransactionCommitRollback},
		{"Transaction/CreateTable", TestTransactionCreateTable},
		{"Transaction/DropTable", TestTransactionDropTable},
		{"Transaction/DropIndex", TestTransactionDropIndex},
		{"Transaction/Table", TestTransactionTable},
		{"Transaction/CreateIndex", TestTransactionCreateIndex},
		{"Transaction/Index", TestTransactionIndex},
		{"Transaction/Indexes", TestTransactionIndexes},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			test.test(t, builder)
		})
	}
}

// TestEngine runs a list of tests against the provided engine.
func TestEngine(t *testing.T, builder Builder) {
	t.Run("Close", func(t *testing.T) {
		ng, cleanup := builder()
		defer cleanup()

		require.NoError(t, ng.Close())
	})
}

// TestTransactionCommitRollback runs a list of tests to verify Commit and Rollback
// behaviour of transactions created from the given engine.
func TestTransactionCommitRollback(t *testing.T, builder Builder) {
	ng, cleanup := builder()
	defer cleanup()

	t.Run("Commit on read-only transaction should fail", func(t *testing.T) {
		tx, err := ng.Begin(false)
		require.NoError(t, err)
		defer tx.Rollback()

		err = tx.Commit()
		require.Error(t, err)
	})

	t.Run("Commit after rollback should fail", func(t *testing.T) {
		tx, err := ng.Begin(true)
		require.NoError(t, err)
		defer tx.Rollback()

		err = tx.Rollback()
		require.NoError(t, err)

		err = tx.Commit()
		require.Error(t, err)
	})

	t.Run("Rollback after commit should not fail", func(t *testing.T) {
		tx, err := ng.Begin(true)
		require.NoError(t, err)
		defer tx.Rollback()

		err = tx.Commit()
		require.NoError(t, err)

		err = tx.Rollback()
		require.NoError(t, err)
	})

	t.Run("Commit after commit should fail", func(t *testing.T) {
		tx, err := ng.Begin(true)
		require.NoError(t, err)
		defer tx.Rollback()

		err = tx.Commit()
		require.NoError(t, err)

		err = tx.Commit()
		require.Error(t, err)
	})

	t.Run("Rollback after rollback should not fail", func(t *testing.T) {
		tx, err := ng.Begin(false)
		require.NoError(t, err)
		defer tx.Rollback()

		err = tx.Rollback()
		require.NoError(t, err)

		err = tx.Rollback()
		require.NoError(t, err)
	})

	t.Run("Read-Only write attempts", func(t *testing.T) {
		tx, err := ng.Begin(true)
		require.NoError(t, err)

		// create table for testing table methods
		err = tx.CreateTable("table1")
		require.NoError(t, err)

		// create index for testing index methods
		err = tx.CreateIndex("table1", "idx")
		require.NoError(t, err)
		err = tx.Commit()
		require.NoError(t, err)

		// create a new read-only transaction
		tx, err = ng.Begin(false)
		defer tx.Rollback()

		// fetch the table and the index
		tb, err := tx.Table("table1", nil)
		require.NoError(t, err)

		// create index for testing index methods
		idx, err := tx.Index("table1", "idx")
		require.NoError(t, err)

		tests := []struct {
			name string
			err  error
			fn   func(*error)
		}{
			{"CreateTable", engine.ErrTransactionReadOnly, func(err *error) { *err = tx.CreateTable("table") }},
			{"DropTable", engine.ErrTransactionReadOnly, func(err *error) { *err = tx.DropTable("table") }},
			{"CreateIndex", engine.ErrTransactionReadOnly, func(err *error) { *err = tx.CreateIndex("table", "idx") }},
			{"DropIndex", engine.ErrTransactionReadOnly, func(err *error) { *err = tx.DropIndex("table", "idx") }},
			{"TableInsert", engine.ErrTransactionReadOnly, func(err *error) { _, *err = tb.Insert(record.FieldBuffer{}) }},
			{"TableDelete", engine.ErrTransactionReadOnly, func(err *error) { *err = tb.Delete([]byte("id")) }},
			{"TableReplace", engine.ErrTransactionReadOnly, func(err *error) { *err = tb.Replace([]byte("id"), record.FieldBuffer{}) }},
			{"IndexSet", engine.ErrTransactionReadOnly, func(err *error) { *err = idx.Set([]byte("value"), []byte("id")) }},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				var err error
				test.fn(&err)

				require.Equal(t, test.err, err)
			})
		}
	})

	t.Run("Commit / Rollback data persistence", func(t *testing.T) {
		// this test checks if rollback undoes data changes correctly and if commit keeps data correctly
		tests := []struct {
			name    string
			initFn  func(engine.Transaction) error
			writeFn func(engine.Transaction, *error)
			readFn  func(engine.Transaction, *error)
		}{
			{
				"CreateTable",
				nil,
				func(tx engine.Transaction, err *error) { *err = tx.CreateTable("table") },
				func(tx engine.Transaction, err *error) { _, *err = tx.Table("table", record.NewCodec()) },
			},
			{
				"DropTable",
				func(tx engine.Transaction) error { return tx.CreateTable("table") },
				func(tx engine.Transaction, err *error) { *err = tx.DropTable("table") },
				func(tx engine.Transaction, err *error) { *err = tx.CreateTable("table") },
			},
			{
				"CreateIndex",
				nil,
				func(tx engine.Transaction, err *error) {
					er := tx.CreateTable("table")
					if er != nil {
						*err = er
						return
					}

					*err = tx.CreateIndex("table", "idx")
				},
				func(tx engine.Transaction, err *error) { _, *err = tx.Index("table", "idx") },
			},
			{
				"DropIndex",
				func(tx engine.Transaction) error {
					err := tx.CreateTable("table")
					if err != nil {
						return err
					}

					return tx.CreateIndex("table", "idx")
				},
				func(tx engine.Transaction, err *error) { *err = tx.DropIndex("table", "idx") },
				func(tx engine.Transaction, err *error) { *err = tx.CreateIndex("table", "idx") },
			},
		}

		for _, test := range tests {
			t.Run(test.name+"/rollback", func(t *testing.T) {
				ng, cleanup := builder()
				defer cleanup()

				if test.initFn != nil {
					func() {
						tx, err := ng.Begin(true)
						require.NoError(t, err)
						defer tx.Rollback()

						err = test.initFn(tx)
						require.NoError(t, err)
						err = tx.Commit()
						require.NoError(t, err)
					}()
				}

				tx, err := ng.Begin(true)
				require.NoError(t, err)
				defer tx.Rollback()

				test.writeFn(tx, &err)
				require.NoError(t, err)

				err = tx.Rollback()
				require.NoError(t, err)

				tx, err = ng.Begin(true)
				require.NoError(t, err)
				defer tx.Rollback()

				test.readFn(tx, &err)
				require.Error(t, err)
			})
		}

		for _, test := range tests {
			ng, cleanup := builder()
			defer cleanup()

			t.Run(test.name+"/commit", func(t *testing.T) {
				if test.initFn != nil {
					func() {
						tx, err := ng.Begin(true)
						require.NoError(t, err)
						defer tx.Rollback()

						err = test.initFn(tx)
						require.NoError(t, err)
						err = tx.Commit()
						require.NoError(t, err)
					}()
				}

				tx, err := ng.Begin(true)
				require.NoError(t, err)
				defer tx.Rollback()

				test.writeFn(tx, &err)
				require.NoError(t, err)

				err = tx.Commit()
				require.NoError(t, err)

				tx, err = ng.Begin(true)
				require.NoError(t, err)
				defer tx.Rollback()

				test.readFn(tx, &err)
				require.NoError(t, err)
			})
		}
	})

	t.Run("Data should be visible within the same transaction", func(t *testing.T) {
		tests := []struct {
			name    string
			writeFn func(engine.Transaction, *error)
			readFn  func(engine.Transaction, *error)
		}{
			{
				"CreateTable",
				func(tx engine.Transaction, err *error) { *err = tx.CreateTable("table") },
				func(tx engine.Transaction, err *error) { _, *err = tx.Table("table", record.NewCodec()) },
			},
			{
				"CreateIndex",
				func(tx engine.Transaction, err *error) {
					er := tx.CreateTable("table")
					if er != nil {
						*err = er
						return
					}

					*err = tx.CreateIndex("table", "idx")
				},
				func(tx engine.Transaction, err *error) { _, *err = tx.Index("table", "idx") },
			},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				ng, cleanup := builder()
				defer cleanup()

				tx, err := ng.Begin(true)
				require.NoError(t, err)
				defer tx.Rollback()

				test.writeFn(tx, &err)
				require.NoError(t, err)

				test.readFn(tx, &err)
				require.NoError(t, err)
			})
		}
	})
}

// TestTransactionCreateTable verifies CreateTable behaviour.
func TestTransactionCreateTable(t *testing.T, builder Builder) {
	t.Run("Should create a table", func(t *testing.T) {
		ng, cleanup := builder()
		defer cleanup()

		tx, err := ng.Begin(true)
		require.NoError(t, err)
		defer tx.Rollback()

		err = tx.CreateTable("table")
		require.NoError(t, err)

		tb, err := tx.Table("table", record.NewCodec())
		require.NoError(t, err)
		require.NotNil(t, tb)
	})

	t.Run("Should fail if table already exists", func(t *testing.T) {
		ng, cleanup := builder()
		defer cleanup()

		tx, err := ng.Begin(true)
		require.NoError(t, err)
		defer tx.Rollback()

		err = tx.CreateTable("table")
		require.NoError(t, err)
		err = tx.CreateTable("table")
		require.Equal(t, engine.ErrTableAlreadyExists, err)
	})
}

// TestTransactionTable verifies Table behaviour.
func TestTransactionTable(t *testing.T, builder Builder) {
	t.Run("Should fail if table not found", func(t *testing.T) {
		ng, cleanup := builder()
		defer cleanup()

		tx, err := ng.Begin(false)
		require.NoError(t, err)
		defer tx.Rollback()

		_, err = tx.Table("table", record.NewCodec())
		require.Equal(t, engine.ErrTableNotFound, err)
	})

	t.Run("Should return the right table", func(t *testing.T) {
		ng, cleanup := builder()
		defer cleanup()

		tx, err := ng.Begin(true)
		require.NoError(t, err)
		defer tx.Rollback()

		// create two tables
		err = tx.CreateTable("tablea")
		require.NoError(t, err)

		err = tx.CreateTable("tableb")
		require.NoError(t, err)

		// fetch first table
		ta, err := tx.Table("tablea", record.NewCodec())
		require.NoError(t, err)

		// fetch second table
		tb, err := tx.Table("tableb", record.NewCodec())
		require.NoError(t, err)

		// insert data in first table
		rowid, err := ta.Insert(record.FieldBuffer([]field.Field{field.NewInt64("a", 10)}))
		require.NoError(t, err)

		// use ta to fetch data and verify if it's present
		r, err := ta.Record(rowid)
		f, err := r.Field("a")
		require.NoError(t, err)
		require.Equal(t, f.Data, field.EncodeInt64(10))

		// use tb to fetch data and verify it's not present
		_, err = tb.Record(rowid)
		require.Equal(t, table.ErrRecordNotFound, err)
	})
}

// TestTransactionDropTable verifies DropTable behaviour.
func TestTransactionDropTable(t *testing.T, builder Builder) {
	t.Run("Should drop a table", func(t *testing.T) {
		ng, cleanup := builder()
		defer cleanup()

		tx, err := ng.Begin(true)
		require.NoError(t, err)
		defer tx.Rollback()

		err = tx.CreateTable("table")
		require.NoError(t, err)

		err = tx.DropTable("table")
		require.NoError(t, err)

		_, err = tx.Table("table", record.NewCodec())
		require.Equal(t, engine.ErrTableNotFound, err)
	})

	t.Run("Should fail if table not found", func(t *testing.T) {
		ng, cleanup := builder()
		defer cleanup()

		tx, err := ng.Begin(true)
		require.NoError(t, err)
		defer tx.Rollback()

		err = tx.DropTable("table")
		require.Equal(t, engine.ErrTableNotFound, err)
	})
}

// TestTransactionCreateIndex verifies CreateIndex behaviour.
func TestTransactionCreateIndex(t *testing.T, builder Builder) {
	t.Run("Should create an index", func(t *testing.T) {
		ng, cleanup := builder()
		defer cleanup()

		tx, err := ng.Begin(true)
		require.NoError(t, err)
		defer tx.Rollback()

		err = tx.CreateTable("table")
		require.NoError(t, err)

		err = tx.CreateIndex("table", "idx")
		require.NoError(t, err)

		idx, err := tx.Index("table", "idx")
		require.NoError(t, err)
		require.NotEmpty(t, idx)
	})

	t.Run("Should fail if index already exists", func(t *testing.T) {
		ng, cleanup := builder()
		defer cleanup()

		tx, err := ng.Begin(true)
		require.NoError(t, err)
		defer tx.Rollback()

		err = tx.CreateTable("table")
		require.NoError(t, err)

		err = tx.CreateIndex("table", "idx")
		require.NoError(t, err)

		err = tx.CreateIndex("table", "idx")
		require.Equal(t, engine.ErrIndexAlreadyExists, err)
	})

	t.Run("Should fail if table doesn't exist", func(t *testing.T) {
		ng, cleanup := builder()
		defer cleanup()

		tx, err := ng.Begin(true)
		require.NoError(t, err)
		defer tx.Rollback()

		err = tx.CreateIndex("table", "idx")
		require.Equal(t, engine.ErrTableNotFound, err)
	})
}

// TestTransactionIndex verifies Index behaviour.
func TestTransactionIndex(t *testing.T, builder Builder) {
	t.Run("Should fail if index not found", func(t *testing.T) {
		ng, cleanup := builder()
		defer cleanup()

		tx, err := ng.Begin(true)
		require.NoError(t, err)
		defer tx.Rollback()

		err = tx.CreateTable("table")
		require.NoError(t, err)

		_, err = tx.Index("table", "idx")
		require.Equal(t, engine.ErrIndexNotFound, err)
	})

	t.Run("Should fail if table doesn't exist", func(t *testing.T) {
		ng, cleanup := builder()
		defer cleanup()

		tx, err := ng.Begin(true)
		require.NoError(t, err)
		defer tx.Rollback()

		_, err = tx.Index("table", "idx")
		require.Equal(t, engine.ErrTableNotFound, err)
	})

	t.Run("Should return the right index", func(t *testing.T) {
		ng, cleanup := builder()
		defer cleanup()

		tx, err := ng.Begin(true)
		require.NoError(t, err)
		defer tx.Rollback()

		// create two tables
		err = tx.CreateTable("tablea")
		require.NoError(t, err)
		err = tx.CreateTable("tableb")
		require.NoError(t, err)

		// create four indexes
		createFn := func(table, field string) index.Index {
			err = tx.CreateIndex(table, field)
			require.NoError(t, err)
			idx, err := tx.Index(table, field)
			require.NoError(t, err)
			return idx
		}
		idxaa := createFn("tablea", "idxa")
		idxab := createFn("tablea", "idxb")
		idxba := createFn("tableb", "idxa")
		idxbb := createFn("tableb", "idxb")

		// fetch first index
		res, err := tx.Index("tablea", "idxa")
		require.NoError(t, err)

		// insert data in first index

		err = res.Set([]byte("value"), []byte("rowid"))
		require.NoError(t, err)

		// use idxaa to fetch data and verify if it's present
		value, rowid := idxaa.Cursor().Seek([]byte("value"))
		require.Equal(t, []byte("value"), value)
		require.Equal(t, []byte("rowid"), rowid)

		// use other indexes to fetch data and verify it's not present
		value, _ = idxab.Cursor().Seek([]byte("value"))
		require.Nil(t, value)
		value, _ = idxba.Cursor().Seek([]byte("value"))
		require.Nil(t, value)
		value, _ = idxbb.Cursor().Seek([]byte("value"))
		require.Nil(t, value)
	})
}

// TestTransactionIndexes verifies Indexes behaviour.
func TestTransactionIndexes(t *testing.T, builder Builder) {
	t.Run("Should fail if table doesn't exist", func(t *testing.T) {
		ng, cleanup := builder()
		defer cleanup()

		tx, err := ng.Begin(false)
		require.NoError(t, err)
		defer tx.Rollback()

		_, err = tx.Indexes("table")
		require.Equal(t, engine.ErrTableNotFound, err)
	})

	t.Run("Should return an empty map if no indexes", func(t *testing.T) {
		ng, cleanup := builder()
		defer cleanup()

		tx, err := ng.Begin(true)
		require.NoError(t, err)
		defer tx.Rollback()

		err = tx.CreateTable("table")
		require.NoError(t, err)

		m, err := tx.Indexes("table")
		require.Empty(t, m)
	})

	t.Run("Should return the right indexes", func(t *testing.T) {
		ng, cleanup := builder()
		defer cleanup()

		tx, err := ng.Begin(true)
		require.NoError(t, err)
		defer tx.Rollback()

		err = tx.CreateTable("table")
		require.NoError(t, err)

		// create two indexes for the same table
		err = tx.CreateIndex("table", "idx1")
		require.NoError(t, err)
		err = tx.CreateIndex("table", "idx2")
		require.NoError(t, err)

		m, err := tx.Indexes("table")
		require.NoError(t, err)
		require.Len(t, m, 2)
		require.Contains(t, m, "idx1")
		require.Contains(t, m, "idx2")
	})
}

// TestTransactionDropIndex verifies DropIndex behaviour.
func TestTransactionDropIndex(t *testing.T, builder Builder) {
	t.Run("Should drop an index", func(t *testing.T) {
		ng, cleanup := builder()
		defer cleanup()

		tx, err := ng.Begin(true)
		require.NoError(t, err)
		defer tx.Rollback()

		err = tx.CreateTable("table")
		require.NoError(t, err)

		err = tx.CreateIndex("table", "index")
		require.NoError(t, err)

		err = tx.DropIndex("table", "index")
		require.NoError(t, err)

		_, err = tx.Index("table", "index")
		require.Equal(t, engine.ErrIndexNotFound, err)
	})

	t.Run("Should fail if table not found", func(t *testing.T) {
		ng, cleanup := builder()
		defer cleanup()

		tx, err := ng.Begin(true)
		require.NoError(t, err)
		defer tx.Rollback()

		err = tx.CreateTable("table")
		require.NoError(t, err)

		err = tx.DropIndex("table", "index")
		require.Equal(t, engine.ErrTableNotFound, err)
	})

	t.Run("Should fail if index not found", func(t *testing.T) {
		ng, cleanup := builder()
		defer cleanup()

		tx, err := ng.Begin(true)
		require.NoError(t, err)
		defer tx.Rollback()

		err = tx.CreateTable("table")
		require.NoError(t, err)

		err = tx.DropIndex("table", "index")
		require.Equal(t, engine.ErrIndexNotFound, err)
	})
}
