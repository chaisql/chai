package query_test

import (
	"bytes"
	"database/sql"
	"testing"

	"github.com/genjidb/genji"
	"github.com/genjidb/genji/document"
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
		{"No table, Add", "SELECT 1 + 1", false, `[{"1 + 1":2}]`, nil},
		{"No table, Mult", "SELECT 2 * 3", false, `[{"2 * 3":6}]`, nil},
		{"No table, Div", "SELECT 10 / 6", false, `[{"10 / 6":1}]`, nil},
		{"No table, Mod", "SELECT 10 % 6", false, `[{"10 % 6":4}]`, nil},
		{"No table, BitwiseAnd", "SELECT 10 & 6", false, `[{"10 & 6":2}]`, nil},
		{"No table, BitwiseOr", "SELECT 10 | 6", false, `[{"10 | 6":14}]`, nil},
		{"No table, BitwiseXor", "SELECT 10 ^ 6", false, `[{"10 ^ 6":12}]`, nil},
		{"No table, function pk()", "SELECT pk()", true, ``, nil},
		{"No table, field", "SELECT a", true, ``, nil},
		{"No table, wildcard", "SELECT *", true, ``, nil},
		{"No table, document", "SELECT {a: 1, b: 2 + 1}", false, `[{"{a: 1, b: 2 + 1}":{"a":1,"b":3}}]`, nil},
		{"No cond", "SELECT * FROM test", false, `[{"k":1,"color":"red","size":10,"shape":"square"},{"k":2,"color":"blue","size":10,"weight":100},{"k":3,"height":100,"weight":200}]`, nil},
		{"Multiple wildcards cond", "SELECT *, *, color FROM test", false, `[{"k":1,"color":"red","size":10,"shape":"square","k":1,"color":"red","size":10,"shape":"square","color":"red"},{"k":2,"color":"blue","size":10,"weight":100,"k":2,"color":"blue","size":10,"weight":100,"color":"blue"},{"k":3,"height":100,"weight":200,"k":3,"height":100,"weight":200,"color":null}]`, nil},
		{"With fields", "SELECT color, shape FROM test", false, `[{"color":"red","shape":"square"},{"color":"blue","shape":null},{"color":null,"shape":null}]`, nil},
		{"With expr fields", "SELECT color, color != 'red' AS notred FROM test", false, `[{"color":"red","notred":false},{"color":"blue","notred":true},{"color":null,"notred":null}]`, nil},
		{"With eq op", "SELECT * FROM test WHERE size = 10", false, `[{"k":1,"color":"red","size":10,"shape":"square"},{"k":2,"color":"blue","size":10,"weight":100}]`, nil},
		{"With neq op", "SELECT * FROM test WHERE color != 'red'", false, `[{"k":2,"color":"blue","size":10,"weight":100}]`, nil},
		{"With gt op", "SELECT * FROM test WHERE size > 10", false, `[]`, nil},
		{"With lt op", "SELECT * FROM test WHERE size < 15", false, `[{"k":1,"color":"red","size":10,"shape":"square"},{"k":2,"color":"blue","size":10,"weight":100}]`, nil},
		{"With lte op", "SELECT * FROM test WHERE color <= 'salmon' ORDER BY k ASC", false, `[{"k":1,"color":"red","size":10,"shape":"square"},{"k":2,"color":"blue","size":10,"weight":100}]`, nil},
		{"With add op", "SELECT size + 10 AS s FROM test ORDER BY k", false, `[{"s":20},{"s":20},{"s":null}]`, nil},
		{"With sub op", "SELECT size - 10 AS s FROM test ORDER BY k", false, `[{"s":0},{"s":0},{"s":null}]`, nil},
		{"With mul op", "SELECT size * 10 AS s FROM test ORDER BY k", false, `[{"s":100},{"s":100},{"s":null}]`, nil},
		{"With div op", "SELECT size / 10 AS s FROM test ORDER BY k", false, `[{"s":1},{"s":1},{"s":null}]`, nil},
		{"With IN op", "SELECT color FROM test WHERE color IN ['red', 'purple'] ORDER BY k", false, `[{"color":"red"}]`, nil},
		{"With IN op on PK", "SELECT color FROM test WHERE k IN [1.1, 1.0] ORDER BY k", false, `[{"color":"red"}]`, nil},
		{"With NOT IN op", "SELECT color FROM test WHERE color NOT IN ['red', 'purple'] ORDER BY k", false, `[{"color":"blue"}]`, nil},
		{"With field comparison", "SELECT * FROM test WHERE color < shape", false, `[{"k":1,"color":"red","size":10,"shape":"square"}]`, nil},
		{"With order by", "SELECT * FROM test ORDER BY color", false, `[{"k":3,"height":100,"weight":200},{"k":2,"color":"blue","size":10,"weight":100},{"k":1,"color":"red","size":10,"shape":"square"}]`, nil},
		{"With order by asc", "SELECT * FROM test ORDER BY color ASC", false, `[{"k":3,"height":100,"weight":200},{"k":2,"color":"blue","size":10,"weight":100},{"k":1,"color":"red","size":10,"shape":"square"}]`, nil},
		{"With order by asc numeric", "SELECT * FROM test ORDER BY weight ASC", false, `[{"k":1,"color":"red","size":10,"shape":"square"},{"k":2,"color":"blue","size":10,"weight":100},{"k":3,"height":100,"weight":200}]`, nil},
		{"With order by asc with limit 2", "SELECT * FROM test ORDER BY color LIMIT 2", false, `[{"k":3,"height":100,"weight":200},{"k":2,"color":"blue","size":10,"weight":100}]`, nil},
		{"With order by asc with limit 1", "SELECT * FROM test ORDER BY color LIMIT 1", false, `[{"k":3,"height":100,"weight":200}]`, nil},
		{"With order by asc with offset", "SELECT * FROM test ORDER BY color OFFSET 1", false, `[{"k":2,"color":"blue","size":10,"weight":100},{"k":1,"color":"red","size":10,"shape":"square"}]`, nil},
		{"With order by asc with limit offset", "SELECT * FROM test ORDER BY color LIMIT 1 OFFSET 1", false, `[{"k":2,"color":"blue","size":10,"weight":100}]`, nil},
		{"With order by desc", "SELECT * FROM test ORDER BY color DESC", false, `[{"k":1,"color":"red","size":10,"shape":"square"},{"k":2,"color":"blue","size":10,"weight":100},{"k":3,"height":100,"weight":200}]`, nil},
		{"With order by desc numeric", "SELECT * FROM test ORDER BY weight DESC", false, `[{"k":3,"height":100,"weight":200},{"k":2,"color":"blue","size":10,"weight":100},{"k":1,"color":"red","size":10,"shape":"square"}]`, nil},
		{"With order by desc with limit", "SELECT * FROM test ORDER BY color DESC LIMIT 2", false, `[{"k":1,"color":"red","size":10,"shape":"square"},{"k":2,"color":"blue","size":10,"weight":100}]`, nil},
		{"With order by desc with offset", "SELECT * FROM test ORDER BY color DESC OFFSET 1", false, `[{"k":2,"color":"blue","size":10,"weight":100},{"k":3,"height":100,"weight":200}]`, nil},
		{"With order by desc with limit offset", "SELECT * FROM test ORDER BY color DESC LIMIT 1 OFFSET 1", false, `[{"k":2,"color":"blue","size":10,"weight":100}]`, nil},
		{"With order by pk asc", "SELECT * FROM test ORDER BY k ASC", false, `[{"k":1,"color":"red","size":10,"shape":"square"},{"k":2,"color":"blue","size":10,"weight":100},{"k":3,"height":100,"weight":200}]`, nil},
		{"With order by pk desc", "SELECT * FROM test ORDER BY k DESC", false, `[{"k":3,"height":100,"weight":200},{"k":2,"color":"blue","size":10,"weight":100},{"k":1,"color":"red","size":10,"shape":"square"}]`, nil},
		{"With order by and where", "SELECT * FROM test WHERE color != 'blue' ORDER BY color DESC LIMIT 1", false, `[{"k":1,"color":"red","size":10,"shape":"square"}]`, nil},
		{"With limit", "SELECT * FROM test WHERE size = 10 LIMIT 1", false, `[{"k":1,"color":"red","size":10,"shape":"square"}]`, nil},
		{"With offset", "SELECT *, pk() FROM test WHERE size = 10 OFFSET 1", false, `[{"pk()":2,"color":"blue","size":10,"weight":100,"k":2}]`, nil},
		{"With limit then offset", "SELECT * FROM test WHERE size = 10 LIMIT 1 OFFSET 1", false, `[{"k":2,"color":"blue","size":10,"weight":100,"k":2}]`, nil},
		{"With offset then limit", "SELECT * FROM test WHERE size = 10 OFFSET 1 LIMIT 1", true, "", nil},
		{"With positional params", "SELECT * FROM test WHERE color = ? OR height = ?", false, `[{"k":1,"color":"red","size":10,"shape":"square"},{"k":3,"height":100,"weight":200}]`, []interface{}{"red", 100}},
		{"With named params", "SELECT * FROM test WHERE color = $a OR height = $d", false, `[{"k":1,"color":"red","size":10,"shape":"square"},{"k":3,"height":100,"weight":200}]`, []interface{}{sql.Named("a", "red"), sql.Named("d", 100)}},
		{"With pk()", "SELECT pk(), color FROM test", false, `[{"pk()":1,"color":"red"},{"pk()":2,"color":"blue"},{"pk()":3,"color":null}]`, []interface{}{sql.Named("a", "red"), sql.Named("d", 100)}},
		{"With pk in cond, gt", "SELECT * FROM test WHERE k > 0 AND weight = 100", false, `[{"k":2,"color":"blue","size":10,"weight":100,"k":2}]`, nil},
		{"With pk in cond, =", "SELECT * FROM test WHERE k = 2.0 AND weight = 100", false, `[{"k":2,"color":"blue","size":10,"weight":100,"k":2}]`, nil},
		{"With two non existing idents, =", "SELECT * FROM test WHERE z = y", false, `[]`, nil},
		{"With two non existing idents, >", "SELECT * FROM test WHERE z > y", false, `[]`, nil},
		{"With two non existing idents, !=", "SELECT * FROM test WHERE z != y", false, `[]`, nil},
	}

	for _, test := range tests {
		testFn := func(withIndexes bool) func(t *testing.T) {
			return func(t *testing.T) {
				db, err := genji.Open(":memory:")
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
						CREATE INDEX idx_weight ON test (weight);
					`)
					require.NoError(t, err)
				}

				err = db.Exec("INSERT INTO test (k, color, size, shape) VALUES (1, 'red', 10, 'square')")
				require.NoError(t, err)
				err = db.Exec("INSERT INTO test (k, color, size, weight) VALUES (2, 'blue', 10, 100)")
				require.NoError(t, err)
				err = db.Exec("INSERT INTO test (k, height, weight) VALUES (3, 100, 200)")
				require.NoError(t, err)

				st, err := db.Query(test.query, test.params...)
				defer st.Close()
				if test.fails {
					require.Error(t, err)
					return
				}
				require.NoError(t, err)

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
		db, err := genji.Open(":memory:")
		require.NoError(t, err)
		defer db.Close()

		err = db.Exec("CREATE TABLE test (foo INTEGER PRIMARY KEY)")
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
		db, err := genji.Open(":memory:")
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
		call("SELECT a[1] FROM test", `{"a[1]": null}`, `{"a[1]": null}`, `{"a[1]": 2}`)
		call("SELECT a[2][1] FROM test", `{"a[2][1]": null}`, `{"a[2][1]": null}`, `{"a[2][1]": 9}`)
	})

	t.Run("table not found", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		require.NoError(t, err)
		defer db.Close()

		err = db.Exec("SELECT * FROM foo")
		require.Error(t, err)
	})

	t.Run("with order by and indexes", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		require.NoError(t, err)
		defer db.Close()

		err = db.Exec("CREATE TABLE test; CREATE INDEX idx_foo ON test(foo);")
		require.NoError(t, err)

		err = db.Exec(`INSERT INTO test (foo) VALUES (1), ('hello'), (2), (true)`)
		require.NoError(t, err)

		st, err := db.Query("SELECT * FROM test ORDER BY foo")
		require.NoError(t, err)
		defer st.Close()

		var buf bytes.Buffer
		err = document.IteratorToJSONArray(&buf, st)
		require.NoError(t, err)
		require.JSONEq(t, `[{"foo": true},{"foo": 1}, {"foo": 2},{"foo": "hello"}]`, buf.String())
	})
}
