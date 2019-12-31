package query_test

import (
	"bytes"
	"database/sql"
	"testing"

	"github.com/asdine/genji"
	"github.com/asdine/genji/document"
	"github.com/asdine/genji/engine/memoryengine"
	"github.com/stretchr/testify/require"
)

func TestSelectStmt(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		fails    bool
		expected string
		params   []interface{}
	}{
		{"No cond", "SELECT * FROM test", false, `[{"k":1,"color":"red","size":10,"shape":"square"},{"k":2,"color":"blue","size":10,"weight":1},{"k":3,"height":100,"weight":20}]`, nil},
		{"Multiple wildcards cond", "SELECT *, *, color FROM test", false, `[{"k":1,"color":"red","size":10,"shape":"square","k":1,"color":"red","size":10,"shape":"square","color":"red"},{"k":2,"color":"blue","size":10,"weight":1,"k":2,"color":"blue","size":10,"weight":1,"color":"blue"},{"k":3,"height":100,"weight":20,"k":3,"height":100,"weight":20,"color":null}]`, nil},
		{"With fields", "SELECT color, shape FROM test", false, `[{"color":"red","shape":"square"},{"color":"blue","shape":null},{"color":null,"shape":null}]`, nil},
		{"With eq cond", "SELECT * FROM test WHERE size = 10", false, `[{"k":1,"color":"red","size":10,"shape":"square"},{"k":2,"color":"blue","size":10,"weight":1}]`, nil},
		{"With neq cond", "SELECT * FROM test WHERE color != 'red'", false, `[{"k":2,"color":"blue","size":10,"weight":1},{"k":3,"height":100,"weight":20}]`, nil},
		{"With gt cond", "SELECT * FROM test WHERE size > 10", false, `[]`, nil},
		{"With lt cond", "SELECT * FROM test WHERE size < 15", false, `[{"k":1,"color":"red","size":10,"shape":"square"},{"k":2,"color":"blue","size":10,"weight":1}]`, nil},
		{"With lte cond", "SELECT * FROM test WHERE color <= 'salmon' ORDER BY k ASC", false, `[{"k":1,"color":"red","size":10,"shape":"square"},{"k":2,"color":"blue","size":10,"weight":1}]`, nil},
		{"With field comparison", "SELECT * FROM test WHERE color < shape", false, `[{"k":1,"color":"red","size":10,"shape":"square"}]`, nil},
		{"With order by", "SELECT * FROM test ORDER BY color", false, `[{"k":3,"height":100,"weight":20},{"k":2,"color":"blue","size":10,"weight":1},{"k":1,"color":"red","size":10,"shape":"square"}]`, nil},
		{"With order by asc", "SELECT * FROM test ORDER BY color ASC", false, `[{"k":3,"height":100,"weight":20},{"k":2,"color":"blue","size":10,"weight":1},{"k":1,"color":"red","size":10,"shape":"square"}]`, nil},
		{"With order by asc with limit 2", "SELECT * FROM test ORDER BY color LIMIT 2", false, `[{"k":3,"height":100,"weight":20},{"k":2,"color":"blue","size":10,"weight":1}]`, nil},
		{"With order by asc with limit 1", "SELECT * FROM test ORDER BY color LIMIT 1", false, `[{"k":3,"height":100,"weight":20}]`, nil},
		{"With order by asc with offset", "SELECT * FROM test ORDER BY color OFFSET 1", false, `[{"k":2,"color":"blue","size":10,"weight":1},{"k":1,"color":"red","size":10,"shape":"square"}]`, nil},
		{"With order by asc with limit offset", "SELECT * FROM test ORDER BY color LIMIT 1 OFFSET 1", false, `[{"k":2,"color":"blue","size":10,"weight":1}]`, nil},
		{"With order by desc", "SELECT * FROM test ORDER BY color DESC", false, `[{"k":1,"color":"red","size":10,"shape":"square"},{"k":2,"color":"blue","size":10,"weight":1},{"k":3,"height":100,"weight":20}]`, nil},
		{"With order by desc with limit", "SELECT * FROM test ORDER BY color DESC LIMIT 2", false, `[{"k":1,"color":"red","size":10,"shape":"square"},{"k":2,"color":"blue","size":10,"weight":1}]`, nil},
		{"With order by desc with offset", "SELECT * FROM test ORDER BY color DESC OFFSET 1", false, `[{"k":2,"color":"blue","size":10,"weight":1},{"k":3,"height":100,"weight":20}]`, nil},
		{"With order by desc with limit offset", "SELECT * FROM test ORDER BY color DESC LIMIT 1 OFFSET 1", false, `[{"k":2,"color":"blue","size":10,"weight":1}]`, nil},
		{"With order by pk asc", "SELECT * FROM test ORDER BY k ASC", false, `[{"k":1,"color":"red","size":10,"shape":"square"},{"k":2,"color":"blue","size":10,"weight":1},{"k":3,"height":100,"weight":20}]`, nil},
		{"With order by pk desc", "SELECT * FROM test ORDER BY k DESC", false, `[{"k":3,"height":100,"weight":20},{"k":2,"color":"blue","size":10,"weight":1},{"k":1,"color":"red","size":10,"shape":"square"}]`, nil},
		{"With order by and where", "SELECT * FROM test WHERE color != 'blue' ORDER BY color DESC LIMIT 1", false, `[{"k":1,"color":"red","size":10,"shape":"square"}]`, nil},
		{"With limit", "SELECT * FROM test WHERE size = 10 LIMIT 1", false, `[{"k":1,"color":"red","size":10,"shape":"square"}]`, nil},
		{"With offset", "SELECT *, key() FROM test WHERE size = 10 OFFSET 1", false, `[{"k":2,"color":"blue","size":10,"weight":1,"k":2}]`, nil},
		{"With limit then offset", "SELECT * FROM test WHERE size = 10 LIMIT 1 OFFSET 1", false, `[{"k":2,"color":"blue","size":10,"weight":1,"k":2}]`, nil},
		{"With offset then limit", "SELECT * FROM test WHERE size = 10 OFFSET 1 LIMIT 1", true, "", nil},
		{"With positional params", "SELECT * FROM test WHERE color = ? OR height = ?", false, `[{"k":1,"color":"red","size":10,"shape":"square"},{"k":3,"height":100,"weight":20}]`, []interface{}{"red", 100}},
		{"With named params", "SELECT * FROM test WHERE color = $a OR height = $d", false, `[{"k":1,"color":"red","size":10,"shape":"square"},{"k":3,"height":100,"weight":20}]`, []interface{}{sql.Named("a", "red"), sql.Named("d", 100)}},
		{"With key()", "SELECT key(), color FROM test", false, `[{"k":1,"color":"red"},{"k":2,"color":"blue"},{"k":3,"color":null}]`, []interface{}{sql.Named("a", "red"), sql.Named("d", 100)}},
		{"With pk in cond, gt", "SELECT * FROM test WHERE k > 0 AND weight = 1", false, `[{"k":2,"color":"blue","size":10,"weight":1,"k":2}]`, nil},
		{"With pk in cond, =", "SELECT * FROM test WHERE k = 2.0 AND weight = 1", false, `[{"k":2,"color":"blue","size":10,"weight":1,"k":2}]`, nil},
		{"With two non existing idents, =", "SELECT * FROM test WHERE z = y", false, `[]`, nil},
		{"With two non existing idents, >", "SELECT * FROM test WHERE z > y", false, `[]`, nil},
		{"With two non existing idents, !=", "SELECT * FROM test WHERE z != y", false, `[{"k":1,"color":"red","size":10,"shape":"square"},{"k":2,"color":"blue","size":10,"weight":1},{"k":3,"height":100,"weight":20}]`, nil},
	}

	for _, test := range tests {
		testFn := func(withIndexes bool) func(t *testing.T) {
			return func(t *testing.T) {
				db, err := genji.New(memoryengine.NewEngine())
				require.NoError(t, err)
				defer db.Close()

				err = db.Exec("CREATE TABLE test (k INTEGER PRIMARY KEY)")
				require.NoError(t, err)
				if withIndexes {
					err = db.Exec(`
						CREATE INDEX idx_color ON test (color);
						CREATE INDEX idx_size ON test (size);
						CREATE INDEX idx_shape ON test (shape);
						CREATE INDEX idx_height ON test (height);
					`)
					require.NoError(t, err)
				}

				err = db.Exec("INSERT INTO test (k, color, size, shape) VALUES (1, 'red', 10, 'square')")
				require.NoError(t, err)
				err = db.Exec("INSERT INTO test (k, color, size, weight) VALUES (2, 'blue', 10, 1)")
				require.NoError(t, err)
				err = db.Exec("INSERT INTO test (k, height, weight) VALUES (3, 100, 20)")
				require.NoError(t, err)

				st, err := db.Query(test.query, test.params...)
				if test.fails {
					require.Error(t, err)
					return
				}
				require.NoError(t, err)
				defer st.Close()

				var buf bytes.Buffer
				err = document.IteratorToJSONArray(&buf, st)
				require.NoError(t, err)
				require.JSONEq(t, test.expected, buf.String())
			}
		}
		t.Run("No Index/"+test.name, testFn(false))
		t.Run("With Index/"+test.name, testFn(true))
	}

	t.Run("with primary key only", func(t *testing.T) {
		db, err := genji.New(memoryengine.NewEngine())
		require.NoError(t, err)
		defer db.Close()

		err = db.Exec("CREATE TABLE test (foo UINT8 PRIMARY KEY)")
		require.NoError(t, err)

		err = db.Exec(`INSERT INTO test (foo, bar) VALUES (1, 'a')`)
		err = db.Exec(`INSERT INTO test (foo, bar) VALUES (2, 'b')`)
		err = db.Exec(`INSERT INTO test (foo, bar) VALUES (3, 'c')`)
		err = db.Exec(`INSERT INTO test (foo, bar) VALUES (4, 'd')`)
		require.NoError(t, err)

		st, err := db.Query("SELECT * FROM test WHERE foo < 400 AND foo >= 2")
		require.NoError(t, err)
		defer st.Close()

		var buf bytes.Buffer
		err = document.IteratorToJSONArray(&buf, st)
		require.NoError(t, err)
		require.JSONEq(t, `[{"foo": 2, "bar": "b"},{"foo": 3, "bar": "c"},{"foo": 4, "bar": "d"}]`, buf.String())
	})

	t.Run("with documents", func(t *testing.T) {
		db, err := genji.New(memoryengine.NewEngine())
		require.NoError(t, err)
		defer db.Close()

		err = db.Exec("CREATE TABLE test")
		require.NoError(t, err)

		err = db.Exec(`INSERT INTO test VALUES {a: {b: 1}}, {a: 1}, {a: [1, 2, [8,9]]}`)
		require.NoError(t, err)

		call := func(q string, res ...string) {
			st, err := db.Query(q)
			require.NoError(t, err)
			defer st.Close()

			var i int
			err = st.Iterate(func(d document.Document) error {
				var buf bytes.Buffer
				err = document.ToJSON(&buf, d)
				require.NoError(t, err)
				require.JSONEq(t, res[i], buf.String())
				i++
				return nil
			})
			require.NoError(t, err)
		}

		call("SELECT *, a.b FROM test WHERE a = {b: 1}", `{"a": {"b":1}, "a.b": 1}`)
		call("SELECT a.b FROM test", `{"a.b": 1}`, `{"a.b": null}`, `{"a.b": null}`)
		call("SELECT a.1 FROM test", `{"a.1": null}`, `{"a.1": null}`, `{"a.1": 2}`)
		call("SELECT a.2.1 FROM test", `{"a.2.1": null}`, `{"a.2.1": null}`, `{"a.2.1": 9}`)
	})

	t.Run("table not found", func(t *testing.T) {
		db, err := genji.New(memoryengine.NewEngine())
		require.NoError(t, err)
		defer db.Close()

		err = db.Exec("SELECT * FROM foo")
		require.Error(t, err)
	})
}
