package statement_test

import (
	"database/sql"
	"strconv"
	"testing"

	_ "github.com/chaisql/chai"
	"github.com/chaisql/chai/internal/testutil"
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
		{"No table, column", "SELECT a", true, ``, nil},
		{"No table, wildcard", "SELECT *", true, ``, nil},
		{"No cond", "SELECT * FROM test", false, `[{"k":1,"color":"red","size":10,"shape":"square","height":null,"weight":null},{"k":2,"color":"blue","size":10,"shape":null,"height":null,"weight":100},{"k":3,"color":null,"size":null,"shape":null,"height":100,"weight":200}]`, nil},
		{"No cond Multiple wildcards", "SELECT *, *, color FROM test", false, `[{"k":1,"color":"red","size":10,"shape":"square","height":null,"weight":null,"k":1,"color":"red","size":10,"shape":"square","height":null,"weight":null,"color":"red"},{"k":2,"color":"blue","size":10,"shape":null,"height":null,"weight":100,"k":2,"color":"blue","size":10,"shape":null,"height":null,"weight":100,"color":"blue"},{"k":3,"color":null,"size":null,"shape":null,"height":100,"weight":200,"k":3,"color":null,"size":null,"shape":null,"height":100,"weight":200,"color":null}]`, nil},
		{"With columns", "SELECT color, shape FROM test", false, `[{"color":"red","shape":"square"},{"color":"blue","shape":null},{"color":null,"shape":null}]`, nil},
		{"No cond, wildcard and other column", "SELECT *, color FROM test", false, `[{"k":1,"color":"red","size":10,"shape":"square","height":null,"weight":null,"color":"red"}, {"k":2,"color":"blue","size":10,"shape":null,"height":null,"weight":100,"color":"blue"}, {"k":3,"color":null,"size":null,"shape":null,"height":100,"weight":200,"color":null}]`, nil},
		{"With DISTINCT", "SELECT DISTINCT * FROM test", false, `[{"k":1,"color":"red","size":10,"shape":"square","height":null,"weight":null},{"k":2,"color":"blue","size":10,"shape":null,"height":null,"weight":100},{"k":3,"color":null,"size":null,"shape":null,"height":100,"weight":200}]`, nil},
		{"With DISTINCT and expr", "SELECT DISTINCT 'a' FROM test", false, `[{"\"a\"":"a"}]`, nil},
		{"With expr columns", "SELECT color, color != 'red' AS notred FROM test", false, `[{"color":"red","notred":false},{"color":"blue","notred":true},{"color":null,"notred":null}]`, nil},
		{"With eq op", "SELECT * FROM test WHERE size = 10", false, `[{"k":1,"color":"red","size":10,"shape":"square","height":null,"weight":null},{"k":2,"color":"blue","size":10,"shape":null,"height":null,"weight":100}]`, nil},
		{"With neq op", "SELECT * FROM test WHERE color != 'red'", false, `[{"k":2,"color":"blue","size":10,"shape":null,"height":null,"weight":100}]`, nil},
		{"With gt op", "SELECT * FROM test WHERE size > 10", false, `null`, nil},
		{"With gt bis", "SELECT * FROM test WHERE size > 9", false, `[{"k":1,"color":"red","size":10,"shape":"square","height":null,"weight":null},{"k":2,"color":"blue","size":10,"shape":null,"height":null,"weight":100}]`, nil},
		{"With lt op", "SELECT * FROM test WHERE size < 15", false, `[{"k":1,"color":"red","size":10,"shape":"square","height":null,"weight":null},{"k":2,"color":"blue","size":10,"shape":null,"height":null,"weight":100}]`, nil},
		{"With lte op", "SELECT * FROM test WHERE color <= 'salmon' ORDER BY k ASC", false, `[{"k":1,"color":"red","size":10,"shape":"square","height":null,"weight":null},{"k":2,"color":"blue","size":10,"shape":null,"height":null,"weight":100}]`, nil},
		{"With add op", "SELECT size + 10 AS s FROM test ORDER BY k", false, `[{"s":20},{"s":20},{"s":null}]`, nil},
		{"With sub op", "SELECT size - 10 AS s FROM test ORDER BY k", false, `[{"s":0},{"s":0},{"s":null}]`, nil},
		{"With mul op", "SELECT size * 10 AS s FROM test ORDER BY k", false, `[{"s":100},{"s":100},{"s":null}]`, nil},
		{"With div op", "SELECT size / 10 AS s FROM test ORDER BY k", false, `[{"s":1},{"s":1},{"s":null}]`, nil},
		{"With IN op", "SELECT color FROM test WHERE color IN ('red', 'purple') ORDER BY k", false, `[{"color":"red"}]`, nil},
		{"With IN op on PK", "SELECT color FROM test WHERE k IN (1.1, 1.0) ORDER BY k", false, `[{"color":"red"}]`, nil},
		{"With NOT IN op", "SELECT color FROM test WHERE color NOT IN ('red', 'purple') ORDER BY k", false, `[{"color":"blue"}]`, nil},
		{"With column comparison", "SELECT * FROM test WHERE color < shape", false, `[{"k":1,"color":"red","size":10,"shape":"square","height":null,"weight":null}]`, nil},
		{"With group by", "SELECT color FROM test GROUP BY color", false, `[{"color":null},{"color":"blue"},{"color":"red"}]`, nil},
		{"With group by expr", "SELECT weight / 2 as half FROM test GROUP BY weight / 2", false, `[{"half":null},{"half":50},{"half":100}]`, nil},
		{"With group by and count", "SELECT COUNT(k) FROM test GROUP BY size", false, `[{"COUNT(k)":1},{"COUNT(k)":2}]`, nil},
		{"With group by and count wildcard", "SELECT COUNT(*  ) FROM test GROUP BY size", false, `[{"COUNT(*)":1},{"COUNT(*)":2}]`, nil},
		{"With order by", "SELECT * FROM test ORDER BY color", false, `[{"k":3,"color":null,"size":null,"shape":null,"height":100,"weight":200},{"k":2,"color":"blue","size":10,"shape":null,"height":null,"weight":100},{"k":1,"color":"red","size":10,"shape":"square","height":null,"weight":null}]`, nil},
		{"With invalid group by / wildcard", "SELECT * FROM test WHERE age = 10 GROUP BY a.b.c", true, ``, nil},
		{"With invalid group by / a.b", "SELECT a.b FROM test WHERE age = 10 GROUP BY a.b.c", true, ``, nil},
		{"With order by", "SELECT * FROM test ORDER BY color", false, `[{"k":3,"color":null,"size":null,"shape":null,"height":100,"weight":200},{"k":2,"color":"blue","size":10,"shape":null,"height":null,"weight":100},{"k":1,"color":"red","size":10,"shape":"square","height":null,"weight":null}]`, nil},
		{"With order by asc", "SELECT * FROM test ORDER BY color ASC", false, `[{"k":3,"color":null,"size":null,"shape":null,"height":100,"weight":200},{"k":2,"color":"blue","size":10,"shape":null,"height":null,"weight":100},{"k":1,"color":"red","size":10,"shape":"square","height":null,"weight":null}]`, nil},
		{"With order by asc numeric", "SELECT * FROM test ORDER BY weight ASC", false, `[{"k":1,"color":"red","size":10,"shape":"square","height":null,"weight":null},{"k":2,"color":"blue","size":10,"shape":null,"height":null,"weight":100},{"k":3,"color":null,"size":null,"shape":null,"height":100,"weight":200}]`, nil},
		{"With order by asc with limit 2", "SELECT * FROM test ORDER BY color LIMIT 2", false, `[{"k":3,"color":null,"size":null,"shape":null,"height":100,"weight":200},{"k":2,"color":"blue","size":10,"shape":null,"height":null,"weight":100}]`, nil},
		{"With order by asc with limit 1", "SELECT * FROM test ORDER BY color LIMIT 1", false, `[{"k":3,"color":null,"size":null,"shape":null,"height":100,"weight":200}]`, nil},
		{"With order by asc with offset", "SELECT * FROM test ORDER BY color OFFSET 1", false, `[{"k":2,"color":"blue","size":10,"shape":null,"height":null,"weight":100},{"k":1,"color":"red","size":10,"shape":"square","height":null,"weight":null}]`, nil},
		{"With order by asc with limit offset", "SELECT * FROM test ORDER BY color LIMIT 1 OFFSET 1", false, `[{"k":2,"color":"blue","size":10,"shape":null,"height":null,"weight":100}]`, nil},
		{"With order by desc", "SELECT * FROM test ORDER BY color DESC", false, `[{"k":1,"color":"red","size":10,"shape":"square","height":null,"weight":null},{"k":2,"color":"blue","size":10,"shape":null,"height":null,"weight":100},{"k":3,"color":null,"size":null,"shape":null,"height":100,"weight":200}]`, nil},
		{"With order by desc numeric", "SELECT * FROM test ORDER BY weight DESC", false, `[{"k":3,"color":null,"size":null,"shape":null,"height":100,"weight":200},{"k":2,"color":"blue","size":10,"shape":null,"height":null,"weight":100},{"k":1,"color":"red","size":10,"shape":"square","height":null,"weight":null}]`, nil},
		{"With order by desc with limit", "SELECT * FROM test ORDER BY color DESC LIMIT 2", false, `[{"k":1,"color":"red","size":10,"shape":"square","height":null,"weight":null},{"k":2,"color":"blue","size":10,"shape":null,"height":null,"weight":100}]`, nil},
		{"With order by desc with offset", "SELECT * FROM test ORDER BY color DESC OFFSET 1", false, `[{"k":2,"color":"blue","size":10,"shape":null,"height":null,"weight":100},{"k":3,"color":null,"size":null,"shape":null,"height":100,"weight":200}]`, nil},
		{"With order by desc with limit offset", "SELECT * FROM test ORDER BY color DESC LIMIT 1 OFFSET 1", false, `[{"k":2,"color":"blue","size":10,"shape":null,"height":null,"weight":100}]`, nil},
		{"With order by pk asc", "SELECT * FROM test ORDER BY k ASC", false, `[{"k":1,"color":"red","size":10,"shape":"square","height":null,"weight":null},{"k":2,"color":"blue","size":10,"shape":null,"height":null,"weight":100},{"k":3,"color":null,"size":null,"shape":null,"height":100,"weight":200}]`, nil},
		{"With order by pk desc", "SELECT * FROM test ORDER BY k DESC", false, `[{"k":3,"color":null,"size":null,"shape":null,"height":100,"weight":200},{"k":2,"color":"blue","size":10,"shape":null,"height":null,"weight":100},{"k":1,"color":"red","size":10,"shape":"square","height":null,"weight":null}]`, nil},
		{"With order by and where", "SELECT * FROM test WHERE color != 'blue' ORDER BY color DESC LIMIT 1", false, `[{"k":1,"color":"red","size":10,"shape":"square","height":null,"weight":null}]`, nil},
		{"With limit", "SELECT * FROM test WHERE size = 10 LIMIT 1", false, `[{"k":1,"color":"red","size":10,"shape":"square","height":null,"weight":null}]`, nil},
		{"With offset", "SELECT * FROM test WHERE size = 10 OFFSET 1", false, `[{"k":2,"color":"blue","size":10,"shape":null,"height":null,"weight":100}]`, nil},
		{"With limit then offset", "SELECT * FROM test WHERE size = 10 LIMIT 1 OFFSET 1", false, `[{"k":2,"color":"blue","size":10,"shape":null,"height":null,"weight":100}]`, nil},
		{"With offset then limit", "SELECT * FROM test WHERE size = 10 OFFSET 1 LIMIT 1", true, "", nil},
		{"With positional params", "SELECT * FROM test WHERE color = $1 OR height = $2", false, `[{"k":1,"color":"red","size":10,"shape":"square","height":null,"weight":null},{"k":3,"color":null,"size":null,"shape":null,"height":100,"weight":200}]`, []interface{}{"red", 100}},
		{"With pk()", "SELECT color FROM test", false, `[{"color":"red"},{"color":"blue"},{"color":null}]`, []interface{}{sql.Named("a", "red"), sql.Named("d", 100)}},
		{"With pk in cond, gt", "SELECT * FROM test WHERE k > 0 AND weight = 100", false, `[{"k":2,"color":"blue","size":10,"shape":null,"height":null,"weight":100}]`, nil},
		{"With pk in cond, =", "SELECT * FROM test WHERE k = 2.0 AND weight = 100", false, `[{"k":2,"color":"blue","size":10,"shape":null,"height":null,"weight":100}]`, nil},
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
		{"Invalid use of MIN() aggregator", "SELECT * FROM test LIMIT min(0)", true, ``, nil},
		{"Invalid use of COUNT() aggregator", "SELECT * FROM test OFFSET count(*)", true, ``, nil},
		{"Invalid use of MAX() aggregator", "SELECT * FROM test LIMIT max(0)", true, ``, nil},
		{"Invalid use of SUM() aggregator", "SELECT * FROM test LIMIT sum(0)", true, ``, nil},
		{"Invalid use of AVG() aggregator", "SELECT * FROM test LIMIT avg(0)", true, ``, nil},
	}

	for _, test := range tests {
		testFn := func(withIndexes bool) func(t *testing.T) {
			return func(t *testing.T) {
				db, err := sql.Open("chai", ":memory:")
				require.NoError(t, err)
				defer db.Close()

				_, err = db.Exec(`--sql
				CREATE TABLE test (
					k INTEGER PRIMARY KEY,
					color TEXT,
					size INTEGER,
					shape TEXT,
					height INTEGER,
					weight INTEGER
				)`)
				require.NoError(t, err)
				if withIndexes {
					_, err = db.Exec(`
						CREATE INDEX idx_color ON test (color);
						CREATE INDEX idx_size ON test (size);
						CREATE INDEX idx_shape ON test (shape);
						CREATE INDEX idx_height ON test (height);
						CREATE INDEX idx_weight ON test (weight);
					`)
					require.NoError(t, err)
				}

				_, err = db.Exec("INSERT INTO test (k, color, size, shape) VALUES (1, 'red', 10, 'square')")
				require.NoError(t, err)
				_, err = db.Exec("INSERT INTO test (k, color, size, weight) VALUES (2, 'blue', 10, 100)")
				require.NoError(t, err)
				_, err = db.Exec("INSERT INTO test (k, height, weight) VALUES (3, 100, 200)")
				require.NoError(t, err)

				rows, err := db.Query(test.query, test.params...)
				if test.fails {
					require.Error(t, err)
					return
				}
				require.NoError(t, err)

				testutil.RequireJSONArrayEq(t, rows, test.expected)
			}
		}
		t.Run("No Index/"+test.name, testFn(false))
		t.Run("With Index/"+test.name, testFn(true))
	}

	t.Run("with primary key only", func(t *testing.T) {
		db, err := sql.Open("chai", ":memory:")
		require.NoError(t, err)
		defer db.Close()

		_, err = db.Exec("CREATE TABLE test (foo INTEGER PRIMARY KEY, bar TEXT)")
		require.NoError(t, err)
		_, err = db.Exec(`INSERT INTO test (foo, bar) VALUES (1, 'a')`)
		require.NoError(t, err)
		_, err = db.Exec(`INSERT INTO test (foo, bar) VALUES (2, 'b')`)
		require.NoError(t, err)
		_, err = db.Exec(`INSERT INTO test (foo, bar) VALUES (3, 'c')`)
		require.NoError(t, err)
		_, err = db.Exec(`INSERT INTO test (foo, bar) VALUES (4, 'd')`)
		require.NoError(t, err)

		rows, err := db.Query("SELECT * FROM test WHERE foo < 400 AND foo >= 2")
		require.NoError(t, err)

		testutil.RequireJSONArrayEq(t, rows, `[{"foo": 2, "bar": "b"},{"foo": 3, "bar": "c"},{"foo": 4, "bar": "d"}]`)
	})

	t.Run("table not found", func(t *testing.T) {
		db, err := sql.Open("chai", ":memory:")
		require.NoError(t, err)
		defer db.Close()

		_, err = db.Exec("SELECT * FROM foo")
		require.Error(t, err)
	})

	t.Run("with order by and indexes", func(t *testing.T) {
		db, err := sql.Open("chai", ":memory:")
		require.NoError(t, err)
		defer db.Close()

		_, err = db.Exec("CREATE TABLE test(foo INT PRIMARY KEY); CREATE INDEX idx_foo ON test(foo);")
		require.NoError(t, err)

		_, err = db.Exec(`INSERT INTO test (foo) VALUES (4), (2), (1), (3)`)
		require.NoError(t, err)

		rows, err := db.Query("SELECT * FROM test ORDER BY foo")
		require.NoError(t, err)

		testutil.RequireJSONArrayEq(t, rows, `[{"foo": 1},{"foo": 2}, {"foo": 3},{"foo": 4}]`)
	})

	t.Run("empty table with aggregators", func(t *testing.T) {
		db, err := sql.Open("chai", ":memory:")
		require.NoError(t, err)
		defer db.Close()

		_, err = db.Exec("CREATE TABLE test(a INTEGER, b INTEGER, id INTEGER PRIMARY KEY);")
		require.NoError(t, err)

		var maxA, minB, count, sum sql.NullInt64
		err = db.QueryRow("SELECT MAX(a), MIN(b), COUNT(*), SUM(id) FROM test").Scan(&maxA, &minB, &count, &sum)
		require.NoError(t, err)
	})

	t.Run("using sequences in SELECT must open read-write transaction instead of read-only", func(t *testing.T) {
		db, err := sql.Open("chai", ":memory:")
		require.NoError(t, err)
		defer db.Close()

		_, err = db.Exec(`
			CREATE TABLE test(a INT PRIMARY KEY);
			INSERT INTO test (a) VALUES (1);
			CREATE SEQUENCE seq;
		`)
		require.NoError(t, err)

		// normal query
		var a, seq int
		err = db.QueryRow("SELECT a, nextval('seq') FROM test").Scan(&a, &seq)
		require.NoError(t, err)
		require.Equal(t, 1, a)
		require.Equal(t, 1, seq)

		// query with no table
		err = db.QueryRow("SELECT nextval('seq')").Scan(&seq)
		require.NoError(t, err)
		require.Equal(t, 2, seq)
	})

	t.Run("LIMIT / OFFSET with params", func(t *testing.T) {
		db, err := sql.Open("chai", ":memory:")
		require.NoError(t, err)
		defer db.Close()

		_, err = db.Exec(`
			CREATE TABLE test(a INT PRIMARY KEY);
			INSERT INTO test (a) VALUES (1), (2), (3);
		`)
		require.NoError(t, err)

		var a int
		err = db.QueryRow("SELECT a FROM test LIMIT $1 OFFSET $2", 1, 1).Scan(&a)
		require.NoError(t, err)
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
		{`double precision`, func(i, notUniqueCount int) (unique interface{}, notunique interface{}) {
			return float64(i), float64(i % notUniqueCount)
		}},
		{`text`, func(i, notUniqueCount int) (unique interface{}, notunique interface{}) {
			return strconv.Itoa(i), strconv.Itoa(i % notUniqueCount)
		}},
	}

	for _, typ := range tps {
		total := 100
		notUnique := total / 10

		t.Run(typ.name, func(t *testing.T) {
			db, err := sql.Open("chai", ":memory:")
			require.NoError(t, err)
			defer db.Close()

			tx, err := db.Begin()
			require.NoError(t, err)
			defer tx.Rollback()

			_, err = tx.Exec("CREATE TABLE test(a " + typ.name + " PRIMARY KEY, b " + typ.name + ", c TEXT, nullable " + typ.name + ");")
			require.NoError(t, err)

			_, err = tx.Exec("CREATE UNIQUE INDEX test_c_index ON test(c);")
			require.NoError(t, err)

			for i := 0; i < total; i++ {
				unique, nonunique := typ.generateValue(i, notUnique)
				_, err = tx.Exec(`INSERT INTO test VALUES ($1, $2, $3, null)`, unique, nonunique, unique)
				require.NoError(t, err)
			}
			err = tx.Commit()
			require.NoError(t, err)

			tests := []struct {
				name          string
				query         string
				expectedCount int
			}{
				{`unique`, `SELECT DISTINCT a FROM test`, total},
				{`non-unique`, `SELECT DISTINCT b FROM test`, notUnique},
				{`null`, `SELECT DISTINCT nullable FROM test`, 1},
				{`wildcard`, `SELECT DISTINCT * FROM test`, total},
				{`literal`, `SELECT DISTINCT 'a' FROM test`, 1},
			}

			for _, test := range tests {
				t.Run(test.name, func(t *testing.T) {
					rows, err := db.Query(test.query)
					require.NoError(t, err)
					defer rows.Close()

					var i int
					for rows.Next() {
						i++
					}
					err = rows.Err()
					require.NoError(t, err)
					require.Equal(t, test.expectedCount, i)
				})
			}
		})
	}
}
