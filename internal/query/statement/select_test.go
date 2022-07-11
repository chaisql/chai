package statement_test

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/genjidb/genji"
	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/internal/testutil"
	"github.com/genjidb/genji/internal/testutil/assert"
	"github.com/genjidb/genji/types"
)

func TestSelectStmt(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		fails    bool
		expected string
		params   []interface{}
	}{
		// {"No table, Add", "SELECT 1 + 1", false, `[{"1 + 1":2}]`, nil},
		// {"No table, Mult", "SELECT 2 * 3", false, `[{"2 * 3":6}]`, nil},
		// {"No table, Div", "SELECT 10 / 6", false, `[{"10 / 6":1}]`, nil},
		// {"No table, Mod", "SELECT 10 % 6", false, `[{"10 % 6":4}]`, nil},
		// {"No table, BitwiseAnd", "SELECT 10 & 6", false, `[{"10 & 6":2}]`, nil},
		// {"No table, BitwiseOr", "SELECT 10 | 6", false, `[{"10 | 6":14}]`, nil},
		// {"No table, BitwiseXor", "SELECT 10 ^ 6", false, `[{"10 ^ 6":12}]`, nil},
		// {"No table, function pk()", "SELECT pk()", false, `[{"pk()":[n]ull}]`, nil},
		// {"No table, field", "SELECT a", true, ``, nil},
		// {"No table, wildcard", "SELECT *", true, ``, nil},
		// {"No table, document", "SELECT {a: 1, b: 2 + 1}", false, `[{"{a: 1, b: 2 + 1}":{"a":1,"b":3}}]`, nil},
		// {"No cond", "SELECT * FROM test", false, `[{"k":1,"color":"red","size":10,"shape":"square"},{"k":2,"color":"blue","size":10,"weight":100},{"k":3,"height":100,"weight":200}]`, nil},
		// {"No cond Multiple wildcards", "SELECT *, *, color FROM test", false, `[{"k":1,"color":"red","size":10,"shape":"square","k":1,"color":"red","size":10,"shape":"square","color":"red"},{"k":2,"color":"blue","size":10,"weight":100,"k":2,"color":"blue","size":10,"weight":100,"color":"blue"},{"k":3,"height":100,"weight":200,"k":3,"height":100,"weight":200,"color":null}]`, nil},
		// {"With fields", "SELECT color, shape FROM test", false, `[{"color":"red","shape":"square"},{"color":"blue","shape":null},{"color":null,"shape":null}]`, nil},
		// {"No cond, wildcard and other field", "SELECT *, color FROM test", false, `[{"color": "red", "k": 1, "color": "red", "size": 10, "shape": "square"}, {"color": "blue", "k": 2, "color": "blue", "size": 10, "weight": 100}, {"color": null, "k": 3, "height": 100, "weight": 200}]`, nil},
		// {"With DISTINCT", "SELECT DISTINCT * FROM test", false, `[{"k":1,"color":"red","size":10,"shape":"square"},{"k":2,"color":"blue","size":10,"weight":100},{"k":3,"height":100,"weight":200}]`, nil},
		// {"With DISTINCT and expr", "SELECT DISTINCT 'a' FROM test", false, `[{"\"a\"":"a"}]`, nil},
		// {"With expr fields", "SELECT color, color != 'red' AS notred FROM test", false, `[{"color":"red","notred":false},{"color":"blue","notred":true},{"color":null,"notred":null}]`, nil},
		// {"With eq op", "SELECT * FROM test WHERE size = 10", false, `[{"k":1,"color":"red","size":10,"shape":"square"},{"k":2,"color":"blue","size":10,"weight":100}]`, nil},
		// {"With neq op", "SELECT * FROM test WHERE color != 'red'", false, `[{"k":2,"color":"blue","size":10,"weight":100}]`, nil},
		// {"With gt op", "SELECT * FROM test WHERE size > 10", false, `[]`, nil},
		// {"With gt bis", "SELECT * FROM test WHERE size > 9", false, `[{"k":1,"color":"red","size":10,"shape":"square"},{"k":2,"color":"blue","size":10,"weight":100}]`, nil},
		// {"With lt op", "SELECT * FROM test WHERE size < 15", false, `[{"k":1,"color":"red","size":10,"shape":"square"},{"k":2,"color":"blue","size":10,"weight":100}]`, nil},
		// {"With lte op", "SELECT * FROM test WHERE color <= 'salmon' ORDER BY k ASC", false, `[{"k":1,"color":"red","size":10,"shape":"square"},{"k":2,"color":"blue","size":10,"weight":100}]`, nil},
		// {"With add op", "SELECT size + 10 AS s FROM test ORDER BY k", false, `[{"s":20},{"s":20},{"s":null}]`, nil},
		// {"With sub op", "SELECT size - 10 AS s FROM test ORDER BY k", false, `[{"s":0},{"s":0},{"s":null}]`, nil},
		// {"With mul op", "SELECT size * 10 AS s FROM test ORDER BY k", false, `[{"s":100},{"s":100},{"s":null}]`, nil},
		// {"With div op", "SELECT size / 10 AS s FROM test ORDER BY k", false, `[{"s":1},{"s":1},{"s":null}]`, nil},
		// {"With IN op", "SELECT color FROM test WHERE color IN ['red', 'purple'] ORDER BY k", false, `[{"color":"red"}]`, nil},
		// {"With IN op on PK", "SELECT color FROM test WHERE k IN [1.1, 1.0] ORDER BY k", false, `[{"color":"red"}]`, nil},
		// {"With NOT IN op", "SELECT color FROM test WHERE color NOT IN ['red', 'purple'] ORDER BY k", false, `[{"color":"blue"}]`, nil},
		{"With field comparison", "SELECT * FROM test WHERE color < shape", false, `[{"k":1,"color":"red","size":10,"shape":"square"}]`, nil},
		{"With group by", "SELECT color FROM test GROUP BY color", false, `[{"color":null},{"color":"blue"},{"color":"red"}]`, nil},
		{"With group by expr", "SELECT weight / 2 as half FROM test GROUP BY weight / 2", false, `[{"half":null},{"half":50},{"half":100}]`, nil},
		{"With group by and count", "SELECT COUNT(k) FROM test GROUP BY size", false, `[{"COUNT(k)":1},{"COUNT(k)":2}]`, nil},
		{"With group by and count wildcard", "SELECT COUNT(*  ) FROM test GROUP BY size", false, `[{"COUNT(*)":1},{"COUNT(*)":2}]`, nil},
		{"With order by", "SELECT * FROM test ORDER BY color", false, `[{"k":3,"height":100,"weight":200},{"k":2,"color":"blue","size":10,"weight":100},{"k":1,"color":"red","size":10,"shape":"square"}]`, nil},
		{"With invalid group by / wildcard", "SELECT * FROM test WHERE age = 10 GROUP BY a.b.c", true, ``, nil},
		{"With invalid group by / a.b", "SELECT a.b FROM test WHERE age = 10 GROUP BY a.b.c", true, ``, nil},
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
		{"With offset", "SELECT *, pk() FROM test WHERE size = 10 OFFSET 1", false, `[{"pk()":[2],"color":"blue","size":10,"weight":100,"k":2}]`, nil},
		{"With limit then offset", "SELECT * FROM test WHERE size = 10 LIMIT 1 OFFSET 1", false, `[{"k":2,"color":"blue","size":10,"weight":100,"k":2}]`, nil},
		{"With offset then limit", "SELECT * FROM test WHERE size = 10 OFFSET 1 LIMIT 1", true, "", nil},
		{"With positional params", "SELECT * FROM test WHERE color = ? OR height = ?", false, `[{"k":1,"color":"red","size":10,"shape":"square"},{"k":3,"height":100,"weight":200}]`, []interface{}{"red", 100}},
		{"With named params", "SELECT * FROM test WHERE color = $a OR height = $d", false, `[{"k":1,"color":"red","size":10,"shape":"square"},{"k":3,"height":100,"weight":200}]`, []interface{}{sql.Named("a", "red"), sql.Named("d", 100)}},
		{"With pk()", "SELECT pk(), color FROM test", false, `[{"pk()":[1],"color":"red"},{"pk()":[2],"color":"blue"},{"pk()":[3],"color":null}]`, []interface{}{sql.Named("a", "red"), sql.Named("d", 100)}},
		{"With pk in cond, gt", "SELECT * FROM test WHERE k > 0 AND weight = 100", false, `[{"k":2,"color":"blue","size":10,"weight":100,"k":2}]`, nil},
		{"With pk in cond, =", "SELECT * FROM test WHERE k = 2.0 AND weight = 100", false, `[{"k":2,"color":"blue","size":10,"weight":100,"k":2}]`, nil},
		{"With count", "SELECT COUNT(k) FROM test", false, `[{"COUNT(k)": 3}]`, nil},
		{"With count wildcard", "SELECT COUNT(*) FROM test", false, `[{"COUNT(*)": 3}]`, nil},
		{"With multiple counts", "SELECT COUNT(k), COUNT(color) FROM test", false, `[{"COUNT(k)": 3, "COUNT(color)": 2}]`, nil},
		{"With min", "SELECT MIN(k) FROM test", false, `[{"MIN(k)": 1}]`, nil},
		{"With multiple mins", "SELECT MIN(color), MIN(weight) FROM test", false, `[{"MIN(color)": "blue", "MIN(weight)": 100}]`, nil},
		{"With max", "SELECT MAX(k) FROM test", false, `[{"MAX(k)": 3}]`, nil},
		{"With multiple maxs", "SELECT MAX(color), MAX(weight) FROM test", false, `[{"MAX(color)": "red", "MAX(weight)": 200}]`, nil},
		{"With sum", "SELECT SUM(k) FROM test", false, `[{"SUM(k)": 6}]`, nil},
		{"With multiple sums", "SELECT SUM(color), SUM(weight) FROM test", false, `[{"SUM(color)": null, "SUM(weight)": 300}]`, nil},
		{"With two non existing idents, =", "SELECT * FROM test WHERE z = y", true, ``, nil},
		{"With two non existing idents, >", "SELECT * FROM test WHERE z > y", true, ``, nil},
		{"With two non existing idents, !=", "SELECT * FROM test WHERE z != y", true, ``, nil},
		// See issue https://github.com/genjidb/genji/issues/283
		{"With empty WHERE and IN", "SELECT * FROM test WHERE [] IN [];", false, `[]`, nil},
		{"Invalid use of MIN() aggregator", "SELECT * FROM test LIMIT min(0)", true, ``, nil},
		{"Invalid use of COUNT() aggregator", "SELECT * FROM test OFFSET count(*)", true, ``, nil},
		{"Invalid use of MAX() aggregator", "SELECT * FROM test LIMIT max(0)", true, ``, nil},
		{"Invalid use of SUM() aggregator", "SELECT * FROM test LIMIT sum(0)", true, ``, nil},
		{"Invalid use of AVG() aggregator", "SELECT * FROM test LIMIT avg(0)", true, ``, nil},
	}

	for _, test := range tests {
		testFn := func(withIndexes bool) func(t *testing.T) {
			return func(t *testing.T) {
				db, err := genji.Open(":memory:")
				assert.NoError(t, err)
				defer db.Close()

				err = db.Exec(`--sql
				CREATE TABLE test (
					k INTEGER PRIMARY KEY,
					color TEXT,
					size INTEGER,
					shape TEXT,
					height INTEGER,
					weight INTEGER
				)`)
				assert.NoError(t, err)
				if withIndexes {
					err = db.Exec(`
						CREATE INDEX idx_color ON test (color);
						CREATE INDEX idx_size ON test (size);
						CREATE INDEX idx_shape ON test (shape);
						CREATE INDEX idx_height ON test (height);
						CREATE INDEX idx_weight ON test (weight);
					`)
					assert.NoError(t, err)
				}

				err = db.Exec("INSERT INTO test (k, color, size, shape) VALUES (1, 'red', 10, 'square')")
				assert.NoError(t, err)
				err = db.Exec("INSERT INTO test (k, color, size, weight) VALUES (2, 'blue', 10, 100)")
				assert.NoError(t, err)
				err = db.Exec("INSERT INTO test (k, height, weight) VALUES (3, 100, 200)")
				assert.NoError(t, err)

				st, err := db.Query(test.query, test.params...)
				defer st.Close()

				if test.fails {
					assert.Error(t, err)
					return
				}

				assert.NoError(t, err)

				var buf bytes.Buffer
				err = testutil.IteratorToJSONArray(&buf, st)
				assert.NoError(t, err)
				require.JSONEq(t, test.expected, buf.String())
			}
		}
		t.Run("No Index/"+test.name, testFn(false))
		t.Run("With Index/"+test.name, testFn(true))
	}

	t.Run("with primary key only", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		assert.NoError(t, err)
		defer db.Close()

		err = db.Exec("CREATE TABLE test (foo INTEGER PRIMARY KEY, bar TEXT)")
		assert.NoError(t, err)

		err = db.Exec(`INSERT INTO test (foo, bar) VALUES (1, 'a')`)
		assert.NoError(t, err)
		err = db.Exec(`INSERT INTO test (foo, bar) VALUES (2, 'b')`)
		assert.NoError(t, err)
		err = db.Exec(`INSERT INTO test (foo, bar) VALUES (3, 'c')`)
		assert.NoError(t, err)
		err = db.Exec(`INSERT INTO test (foo, bar) VALUES (4, 'd')`)
		assert.NoError(t, err)

		st, err := db.Query("SELECT * FROM test WHERE foo < 400 AND foo >= 2")
		assert.NoError(t, err)
		defer st.Close()

		var buf bytes.Buffer
		err = testutil.IteratorToJSONArray(&buf, st)
		assert.NoError(t, err)
		require.JSONEq(t, `[{"foo": 2, "bar": "b"},{"foo": 3, "bar": "c"},{"foo": 4, "bar": "d"}]`, buf.String())
	})

	t.Run("with documents", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		assert.NoError(t, err)
		defer db.Close()

		err = db.Exec("CREATE TABLE test")
		assert.NoError(t, err)

		err = db.Exec(`INSERT INTO test VALUES {a: {b: 1}}, {a: 1}, {a: [1, 2, [8,9]]}`)
		assert.NoError(t, err)

		call := func(q string, res ...string) {
			st, err := db.Query(q)
			assert.NoError(t, err)
			defer st.Close()

			var i int
			err = st.Iterate(func(d types.Document) error {
				data, err := document.MarshalJSON(d)
				assert.NoError(t, err)
				require.JSONEq(t, res[i], string(data))
				i++
				return nil
			})
			assert.NoError(t, err)
		}

		call("SELECT *, a.b FROM test WHERE a = {b: 1}", `{"a": {"b":1}, "a.b": 1}`)
		call("SELECT a.b FROM test", `{"a.b": 1}`, `{"a.b": null}`, `{"a.b": null}`)
		call("SELECT a[1] FROM test", `{"a[1]": null}`, `{"a[1]": null}`, `{"a[1]": 2}`)
		call("SELECT a[2][1] FROM test", `{"a[2][1]": null}`, `{"a[2][1]": null}`, `{"a[2][1]": 9}`)
	})

	t.Run("table not found", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		assert.NoError(t, err)
		defer db.Close()

		err = db.Exec("SELECT * FROM foo")
		assert.Error(t, err)
	})

	t.Run("with unknown field", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		assert.NoError(t, err)
		defer db.Close()

		err = db.Exec("CREATE TABLE test(a INTEGER);")
		assert.NoError(t, err)

		err = db.Exec(`INSERT INTO test (a) VALUES (1)`)
		assert.NoError(t, err)

		st, err := db.Query("SELECT a,b FROM test")
		assert.Error(t, err)
		defer st.Close()
	})

	t.Run("with order by and indexes", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		assert.NoError(t, err)
		defer db.Close()

		err = db.Exec("CREATE TABLE test(foo any); CREATE INDEX idx_foo ON test(foo);")
		assert.NoError(t, err)

		err = db.Exec(`INSERT INTO test (foo) VALUES (1), ('hello'), (2), (true)`)
		assert.NoError(t, err)

		st, err := db.Query("SELECT * FROM test ORDER BY foo")
		assert.NoError(t, err)
		defer st.Close()

		var buf bytes.Buffer
		err = testutil.IteratorToJSONArray(&buf, st)
		assert.NoError(t, err)
		require.JSONEq(t, `[{"foo": true},{"foo": 1}, {"foo": 2},{"foo": "hello"}]`, buf.String())
	})

	// https://github.com/genjidb/genji/issues/208
	t.Run("group by with arrays", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		assert.NoError(t, err)
		defer db.Close()

		err = db.Exec("CREATE TABLE test; INSERT INTO test (a) VALUES ([1, 2, 3]);")
		assert.NoError(t, err)

		d, err := db.QueryDocument("SELECT MAX(a) from test GROUP BY a")
		assert.NoError(t, err)

		enc, err := json.Marshal(d)
		assert.NoError(t, err)

		require.JSONEq(t, `{"MAX(a)": [1, 2, 3]}`, string(enc))
	})

	t.Run("empty table with aggregators", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		assert.NoError(t, err)
		defer db.Close()

		err = db.Exec("CREATE TABLE test;")
		assert.NoError(t, err)

		d, err := db.QueryDocument("SELECT MAX(a), MIN(b), COUNT(*), SUM(id) FROM test")
		assert.NoError(t, err)

		enc, err := json.Marshal(d)
		assert.NoError(t, err)

		require.JSONEq(t, `{"MAX(a)": null, "MIN(b)": null, "COUNT(*)": 0, "SUM(id)": null}`, string(enc))
	})

	t.Run("array number comparison with no constraints", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		assert.NoError(t, err)
		defer db.Close()

		err = db.Exec(`
			CREATE TABLE test(a any);
			INSERT INTO test (a) VALUES ([1,2,3]), ([4, 5, 6]);
		`)
		assert.NoError(t, err)

		check := func() {
			t.Helper()

			d, err := db.QueryDocument("SELECT * FROM test WHERE a = [1,2,3];")
			assert.NoError(t, err)

			enc, err := json.Marshal(d)
			assert.NoError(t, err)

			require.JSONEq(t, `{"a": [1, 2, 3]}`, string(enc))
		}

		check()

		err = db.Exec("CREATE INDEX idx_test_a ON test(a);")
		assert.NoError(t, err)

		check()
	})

	t.Run("using sequences in SELECT must open read-write transaction instead of read-only", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		assert.NoError(t, err)
		defer db.Close()

		err = db.Exec(`
			CREATE TABLE test;
			INSERT INTO test (a) VALUES (1);
			CREATE SEQUENCE seq;
		`)
		assert.NoError(t, err)

		// normal query
		d, err := db.QueryDocument("SELECT a, NEXT VALUE FOR seq FROM test")
		assert.NoError(t, err)
		var a, seq int
		err = document.Scan(d, &a, &seq)
		assert.NoError(t, err)
		require.Equal(t, 1, a)
		require.Equal(t, 1, seq)

		// query with no table
		d, err = db.QueryDocument("SELECT NEXT VALUE FOR seq")
		assert.NoError(t, err)
		err = document.Scan(d, &seq)
		assert.NoError(t, err)
		require.Equal(t, 2, seq)
	})

	t.Run("LIMIT / OFFSET with params", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		assert.NoError(t, err)
		defer db.Close()

		err = db.Exec(`
			CREATE TABLE test;
			INSERT INTO test (a) VALUES (1), (2), (3);
		`)
		assert.NoError(t, err)

		d, err := db.QueryDocument("SELECT a FROM test LIMIT ? OFFSET ?", 1, 1)
		assert.NoError(t, err)
		var a int
		err = document.Scan(d, &a)
		assert.NoError(t, err)
		require.Equal(t, 2, a)
	})
}

func TestDistinct(t *testing.T) {
	tps := []struct {
		name          string
		generateValue func(i, notUniqueCount int) (unique interface{}, notunique interface{})
	}{
		{`integer`, func(i, notUniqueCount int) (unique interface{}, notunique interface{}) {
			return i, i % notUniqueCount
		}},
		{`double`, func(i, notUniqueCount int) (unique interface{}, notunique interface{}) {
			return float64(i), float64(i % notUniqueCount)
		}},
		{`text`, func(i, notUniqueCount int) (unique interface{}, notunique interface{}) {
			return strconv.Itoa(i), strconv.Itoa(i % notUniqueCount)
		}},
		{`array`, func(i, notUniqueCount int) (unique interface{}, notunique interface{}) {
			return []interface{}{i}, []interface{}{i % notUniqueCount}
		}},
	}

	for _, typ := range tps {
		total := 100
		notUnique := total / 10

		t.Run(typ.name, func(t *testing.T) {
			db, err := genji.Open(":memory:")
			assert.NoError(t, err)
			defer db.Close()

			tx, err := db.Begin(true)
			assert.NoError(t, err)
			defer tx.Rollback()

			err = tx.Exec("CREATE TABLE test(a " + typ.name + " PRIMARY KEY, b " + typ.name + ", doc DOCUMENT, nullable " + typ.name + ");")
			assert.NoError(t, err)

			err = tx.Exec("CREATE UNIQUE INDEX test_doc_index ON test(doc);")
			assert.NoError(t, err)

			for i := 0; i < total; i++ {
				unique, nonunique := typ.generateValue(i, notUnique)
				err = tx.Exec(`INSERT INTO test VALUES {a: ?, b: ?, doc: {a: ?, b: ?}, nullable: null}`, unique, nonunique, unique, nonunique)
				assert.NoError(t, err)
			}
			err = tx.Commit()
			assert.NoError(t, err)

			tests := []struct {
				name          string
				query         string
				expectedCount int
			}{
				{`unique`, `SELECT DISTINCT a FROM test`, total},
				{`non-unique`, `SELECT DISTINCT b FROM test`, notUnique},
				{`documents`, `SELECT DISTINCT doc FROM test`, total},
				{`null`, `SELECT DISTINCT nullable FROM test`, 1},
				{`wildcard`, `SELECT DISTINCT * FROM test`, total},
				{`literal`, `SELECT DISTINCT 'a' FROM test`, 1},
				{`pk()`, `SELECT DISTINCT pk() FROM test`, total},
			}

			for _, test := range tests {
				t.Run(test.name, func(t *testing.T) {
					q, err := db.Query(test.query)
					assert.NoError(t, err)
					defer q.Close()

					var i int
					err = q.Iterate(func(d types.Document) error {
						i++
						return nil
					})
					assert.NoError(t, err)
					require.Equal(t, test.expectedCount, i)
				})
			}
		})
	}
}
