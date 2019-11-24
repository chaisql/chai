package genji_test

import (
	"bytes"
	"database/sql"
	"fmt"
	"log"
	"testing"

	"github.com/asdine/genji"
	"github.com/asdine/genji/database"
	"github.com/asdine/genji/engine/memoryengine"
	"github.com/asdine/genji/record"
	"github.com/stretchr/testify/require"
)

func ExampleDB_SQLDB() {
	db, err := genji.New(memoryengine.NewEngine())
	if err != nil {
		log.Fatal(err)
	}
	dbx := db.SQLDB()
	defer db.Close()

	_, err = dbx.Exec("CREATE TABLE IF NOT EXISTS user")
	if err != nil {
		log.Fatal(err)
	}

	_, err = dbx.Exec("CREATE INDEX IF NOT EXISTS idx_user_name ON user (name)")
	if err != nil {
		log.Fatal(err)
	}

	_, err = dbx.Exec("INSERT INTO user (id, name, age) VALUES (?, ?, ?)", 10, "foo", 15)
	if err != nil {
		log.Fatal(err)
	}

	_, err = dbx.Exec("INSERT INTO user RECORDS ?, ?", &User{ID: 1, Name: "bar", Age: 100}, &User{ID: 2, Name: "baz"})
	if err != nil {
		log.Fatal(err)
	}

	rows, err := dbx.Query("SELECT * FROM user WHERE name = ?", "bar")
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	for rows.Next() {
		var u User
		err = rows.Scan(&u)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(u)
	}

	err = rows.Err()
	if err != nil {
		log.Fatal(err)
	}

	// Output: {1 bar 100}
}

func ExampleTx() {
	db, err := genji.New(memoryengine.NewEngine())
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	tx, err := db.Begin(false)
	if err != nil {
		panic(err)
	}
	defer tx.Rollback()

	err = tx.Exec("CREATE TABLE IF NOT EXISTS user")
	if err != nil {
		log.Fatal(err)
	}

	err = tx.Exec("INSERT INTO user (id, name, age) VALUES (?, ?, ?)", 10, "foo", 15)
	if err != nil {
		log.Fatal(err)
	}

	result, err := tx.Query("SELECT id, name, age FROM user WHERE name = ?", "foo")
	if err != nil {
		panic(err)
	}
	defer result.Close()

	var u User
	r, err := result.First()
	if err != nil {
		panic(err)
	}

	err = u.ScanRecord(r)
	if err != nil {
		panic(err)
	}

	fmt.Println(u)

	var id uint64
	var name string
	var age uint8

	err = record.Scan(r, &id, &name, &age)
	if err != nil {
		panic(err)
	}

	fmt.Println(id, name, age)

	err = tx.Commit()
	if err != nil {
		panic(err)
	}

	// Output: {10 foo 15}
	// 10 foo 15
}

func TestQueryRecord(t *testing.T) {
	db, err := genji.New(memoryengine.NewEngine())
	require.NoError(t, err)

	tx, err := db.Begin(true)
	require.NoError(t, err)

	err = tx.Exec(`
			CREATE TABLE test;
			INSERT INTO test (a, b) VALUES (1, 'foo'), (2, 'bar')
		`)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	t.Run("Should return the first record", func(t *testing.T) {
		var a int
		var b string

		r, err := db.QueryRecord("SELECT * FROM test")
		err = record.Scan(r, &a, &b)
		require.NoError(t, err)
		require.Equal(t, 1, a)
		require.Equal(t, "foo", b)

		tx, err := db.Begin(false)
		require.NoError(t, err)
		defer tx.Rollback()

		r, err = tx.QueryRecord("SELECT * FROM test")
		require.NoError(t, err)
		err = record.Scan(r, &a, &b)
		require.NoError(t, err)
		require.Equal(t, 1, a)
		require.Equal(t, "foo", b)
	})

	t.Run("Should return an error if no record", func(t *testing.T) {
		r, err := db.QueryRecord("SELECT * FROM test WHERE a > 100")
		require.Equal(t, genji.ErrRecordNotFound, err)
		require.Nil(t, r)

		tx, err := db.Begin(false)
		require.NoError(t, err)
		defer tx.Rollback()
		r, err = tx.QueryRecord("SELECT * FROM test WHERE a > 100")
		require.Equal(t, genji.ErrRecordNotFound, err)
		require.Nil(t, r)
	})
}

func ExampleResult_First() {
	db, err := genji.New(memoryengine.NewEngine())
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	err = db.Exec("CREATE TABLE IF NOT EXISTS user")
	if err != nil {
		log.Fatal(err)
	}

	err = db.Exec("INSERT INTO user (id, name, age) VALUES (?, ?, ?)", 10, "foo", 15)
	if err != nil {
		log.Fatal(err)
	}

	result, err := db.Query("SELECT * FROM user WHERE name = ?", "foo")
	if err != nil {
		panic(err)
	}
	defer result.Close()

	r, err := result.First()
	if err != nil {
		panic(err)
	}

	// Scan using generated methods
	var u User
	err = u.ScanRecord(r)
	if err != nil {
		panic(err)
	}

	fmt.Println(u)

	// Scan individual variables
	var id uint64
	var name string
	var age uint8

	err = record.Scan(r, &id, &name, &age)
	if err != nil {
		panic(err)
	}

	fmt.Println(id, name, age)

	// Output: {10 foo 15}
	// 10 foo 15
}

func ExampleResult_Iterate() {
	db, err := genji.New(memoryengine.NewEngine())
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	err = db.Exec("CREATE TABLE IF NOT EXISTS user")
	if err != nil {
		log.Fatal(err)
	}

	for i := 1; i <= 10; i++ {
		err = db.Exec("INSERT INTO user RECORDS ?", &User{
			ID:   int64(i),
			Name: fmt.Sprintf("foo%d", i),
			Age:  uint32(i * 10),
		})
		if err != nil {
			log.Fatal(err)
		}
	}

	result, err := db.Query(`SELECT * FROM user WHERE "age" >= 18`)
	if err != nil {
		panic(err)
	}
	defer result.Close()

	err = result.Iterate(func(r record.Record) error {
		// Scan using generated methods
		var u User
		err = u.ScanRecord(r)
		if err != nil {
			return err
		}

		fmt.Println(u)

		// Or scan individual variables
		var id uint64
		var name string
		var age uint8

		err = record.Scan(r, &id, &name, &age)
		if err != nil {
			return err
		}

		fmt.Println(id, name, age)
		return nil
	})
	if err != nil {
		panic(err)
	}

	// Output: {2 foo2 20}
	// 2 foo2 20
	// {3 foo3 30}
	// 3 foo3 30
	// {4 foo4 40}
	// 4 foo4 40
	// {5 foo5 50}
	// 5 foo5 50
	// {6 foo6 60}
	// 6 foo6 60
	// {7 foo7 70}
	// 7 foo7 70
	// {8 foo8 80}
	// 8 foo8 80
	// {9 foo9 90}
	// 9 foo9 90
	// {10 foo10 100}
	// 10 foo10 100
}

func TestCreateTable(t *testing.T) {
	tests := []struct {
		name  string
		query string
		fails bool
	}{
		{"Basic", `CREATE TABLE test`, false},
		{"Exists", "CREATE TABLE test;CREATE TABLE test", true},
		{"If not exists", "CREATE TABLE IF NOT EXISTS test", false},
		{"If not exists, twice", "CREATE TABLE IF NOT EXISTS test;CREATE TABLE IF NOT EXISTS test", false},
		{"With primary key", "CREATE TABLE test(foo STRING PRIMARY KEY)", false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, err := genji.New(memoryengine.NewEngine())
			require.NoError(t, err)
			defer db.Close()

			err = db.Exec(test.query)
			if test.fails {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)

			err = db.ViewTable("test", func(_ *genji.Tx, _ *database.Table) error {
				return nil
			})
			require.NoError(t, err)
		})
	}
}

func TestCreateIndex(t *testing.T) {
	tests := []struct {
		name  string
		query string
		fails bool
	}{
		{"Basic", "CREATE INDEX idx ON test (foo)", false},
		{"If not exists", "CREATE INDEX IF NOT EXISTS idx ON test (foo)", false},
		{"Unique", "CREATE UNIQUE INDEX IF NOT EXISTS idx ON test (foo)", false},
		{"No fields", "CREATE INDEX idx ON test", true},
		{"More than 1 field", "CREATE INDEX idx ON test (foo, bar)", true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, err := genji.New(memoryengine.NewEngine())
			require.NoError(t, err)
			defer db.Close()

			err = db.Exec("CREATE TABLE test")
			require.NoError(t, err)

			err = db.Exec(test.query)
			if test.fails {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
		})
	}
}

func TestDrop(t *testing.T) {
	tests := []struct {
		name  string
		query string
		fails bool
	}{
		{"Drop table", "DROP TABLE test", false},
		{"Drop table If not exists", "DROP TABLE IF EXISTS test", false},
		{"Drop index", "DROP INDEX idx", false},
		{"Drop index if exists", "DROP INDEX IF EXISTS idx", false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, err := genji.New(memoryengine.NewEngine())
			require.NoError(t, err)
			defer db.Close()

			err = db.Exec("CREATE TABLE test; CREATE INDEX idx ON test (foo)")
			require.NoError(t, err)

			err = db.Exec(test.query)
			if test.fails {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
		})
	}
}

func TestDeleteStmt(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		fails    bool
		expected string
		params   []interface{}
	}{
		{"No cond", `DELETE FROM test`, false, "", nil},
		{"With cond", "DELETE FROM test WHERE b = 'bar1'", false, "foo3,bar2,bar3\n", nil},
		{"Table not found", "DELETE FROM foo WHERE b = 'bar1'", true, "", nil},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, err := genji.New(memoryengine.NewEngine())
			require.NoError(t, err)
			defer db.Close()

			err = db.Exec("CREATE TABLE test")
			require.NoError(t, err)
			err = db.Exec("INSERT INTO test (a, b, c) VALUES ('foo1', 'bar1', 'baz1')")
			require.NoError(t, err)
			err = db.Exec("INSERT INTO test (a, b) VALUES ('foo2', 'bar1')")
			require.NoError(t, err)
			err = db.Exec("INSERT INTO test (d, b, e) VALUES ('foo3', 'bar2', 'bar3')")
			require.NoError(t, err)

			err = db.Exec(test.query, test.params...)
			if test.fails {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)

			st, err := db.Query("SELECT * FROM test")
			require.NoError(t, err)
			defer st.Close()

			var buf bytes.Buffer
			err = record.IteratorToCSV(&buf, st)
			require.NoError(t, err)
			require.Equal(t, test.expected, buf.String())
		})
	}
}

func TestInsertStmt(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		fails    bool
		expected string
		params   []interface{}
	}{
		{"Values / No columns", `INSERT INTO test VALUES ("a", 'b', 'c')`, true, ``, nil},
		{"Values / With columns", `INSERT INTO test (a, b, c) VALUES ('a', 'b', 'c')`, false, "1,a,b,c\n", nil},
		{"Values / Ident", `INSERT INTO test (a) VALUES (a)`, true, ``, nil},
		{"Values / Ident string", `INSERT INTO test (a) VALUES ("a")`, true, ``, nil},
		{"Values / With fields ident string", `INSERT INTO test (a, "foo bar") VALUES ('c', 'd')`, false, "1,c,d\n", nil},
		{"Values / Positional Params", "INSERT INTO test (a, b, c) VALUES (?, 'e', ?)", false, "1,d,e,f\n", []interface{}{"d", "f"}},
		{"Values / Named Params", "INSERT INTO test (a, b, c) VALUES ($d, 'e', $f)", false, "1,d,e,f\n", []interface{}{sql.Named("f", "f"), sql.Named("d", "d")}},
		{"Values / Invalid params", "INSERT INTO test (a, b, c) VALUES ('d', ?)", true, "", []interface{}{'e'}},
		{"Values / List", `INSERT INTO test (a, b, c) VALUES ("a", 'b', (1, 2, 3))`, true, "", nil},
		{"Records", "INSERT INTO test RECORDS (a: 'a', b: 2.3, c: 1 = 1)", false, "1,a,2.3,true\n", nil},
		{"Records / Positional Params", "INSERT INTO test RECORDS (a: ?, b: 2.3, c: ?)", false, "1,a,2.3,true\n", []interface{}{"a", true}},
		{"Records / Named Params", "INSERT INTO test RECORDS (a: $a, b: 2.3, c: $c)", false, "1,1,2.3,true\n", []interface{}{sql.Named("c", true), sql.Named("a", 1)}},
		{"Records / List ", "INSERT INTO test RECORDS (a: (1, 2, 3))", true, "", nil},
		{"Records / strings", `INSERT INTO test RECORDS ('a': 'a', b: 2.3)`, true, "", nil},
		{"Records / ident value", `INSERT INTO test RECORDS ("a": "a")`, true, "", nil},
	}

	for _, test := range tests {
		testFn := func(withIndexes bool) func(t *testing.T) {
			return func(t *testing.T) {
				db, err := genji.New(memoryengine.NewEngine())
				require.NoError(t, err)
				defer db.Close()

				err = db.Exec("CREATE TABLE test")
				require.NoError(t, err)
				if withIndexes {
					err = db.Exec(`
						CREATE INDEX idx_a ON test (a);
						CREATE INDEX idx_b ON test (b);
						CREATE INDEX idx_c ON test (c);
					`)
					require.NoError(t, err)
				}
				err = db.Exec(test.query, test.params...)
				if test.fails {
					require.Error(t, err)
					return
				}
				require.NoError(t, err)

				st, err := db.Query("SELECT key(), * FROM test")
				require.NoError(t, err)
				defer st.Close()

				var buf bytes.Buffer
				err = record.IteratorToCSV(&buf, st)
				require.NoError(t, err)
				require.Equal(t, test.expected, buf.String())
			}
		}

		t.Run("No Index/"+test.name, testFn(false))
		t.Run("With Index/"+test.name, testFn(true))
	}

	t.Run("with primary key", func(t *testing.T) {
		db, err := genji.New(memoryengine.NewEngine())
		require.NoError(t, err)
		defer db.Close()

		err = db.Exec("CREATE TABLE test (foo INTEGER PRIMARY KEY)")
		require.NoError(t, err)

		err = db.Exec(`INSERT INTO test (bar) VALUES (1)`)
		require.Error(t, err)
		err = db.Exec(`INSERT INTO test (bar, foo) VALUES (1, 2)`)
		require.NoError(t, err)

		err = db.Exec(`INSERT INTO test (bar, foo) VALUES (1, 2)`)
		require.Equal(t, err, genji.ErrDuplicateRecord)
	})

	t.Run("with shadowing", func(t *testing.T) {
		db, err := genji.New(memoryengine.NewEngine())
		require.NoError(t, err)
		defer db.Close()

		err = db.Exec("CREATE TABLE test")
		require.NoError(t, err)

		err = db.Exec(`INSERT INTO test ("key()", "key") VALUES (1, 2)`)
		require.NoError(t, err)
	})
}

func TestSelectStmt(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		fails    bool
		expected string
		params   []interface{}
	}{
		{"No cond", "SELECT * FROM test", false, "1,foo1,bar1,baz1\n2,foo2,bar1,1\n3,foo3,bar2\n", nil},
		{"Multiple wildcards cond", "SELECT *, *, a FROM test", false, "1,foo1,bar1,baz1,1,foo1,bar1,baz1,foo1\n2,foo2,bar1,1,2,foo2,bar1,1,foo2\n3,foo3,bar2,3,foo3,bar2\n", nil},
		{"With fields", "SELECT a, c FROM test", false, "foo1,baz1\nfoo2\n\n", nil},
		{"With eq cond", "SELECT * FROM test WHERE b = 'bar1'", false, "1,foo1,bar1,baz1\n2,foo2,bar1,1\n", nil},
		{"With neq cond", "SELECT * FROM test WHERE a != 'foo1'", false, "2,foo2,bar1,1\n3,foo3,bar2\n", nil},
		{"With gt cond", "SELECT * FROM test WHERE b > 'bar1'", false, "", nil},
		{"With lt cond", "SELECT * FROM test WHERE a < 'zzzzz'", false, "1,foo1,bar1,baz1\n2,foo2,bar1,1\n", nil},
		{"With lte cond", "SELECT * FROM test WHERE a <= 'foo3'", false, "1,foo1,bar1,baz1\n2,foo2,bar1,1\n", nil},
		{"With field comparison", "SELECT * FROM test WHERE b < a", false, "1,foo1,bar1,baz1\n2,foo2,bar1,1\n", nil},
		{"With limit", "SELECT * FROM test WHERE b = 'bar1' LIMIT 1", false, "1,foo1,bar1,baz1\n", nil},
		{"With offset", "SELECT *, key() FROM test WHERE b = 'bar1' OFFSET 1", false, "2,foo2,bar1,1,2\n", nil},
		{"With limit then offset", "SELECT * FROM test WHERE b = 'bar1' LIMIT 1 OFFSET 1", false, "2,foo2,bar1,1\n", nil},
		{"With offset then limit", "SELECT * FROM test WHERE b = 'bar1' OFFSET 1 LIMIT 1", true, "", nil},
		{"With positional params", "SELECT * FROM test WHERE a = ? OR d = ?", false, "1,foo1,bar1,baz1\n3,foo3,bar2\n", []interface{}{"foo1", "foo3"}},
		{"With named params", "SELECT * FROM test WHERE a = $a OR d = $d", false, "1,foo1,bar1,baz1\n3,foo3,bar2\n", []interface{}{sql.Named("a", "foo1"), sql.Named("d", "foo3")}},
		{"With key()", "SELECT key(), a FROM test", false, "1,foo1\n2,foo2\n3\n", []interface{}{sql.Named("a", "foo1"), sql.Named("d", "foo3")}},
		{"With pk in cond, gt", "SELECT * FROM test WHERE k > 0 AND e = 1", false, "2,foo2,bar1,1\n", nil},
		{"With pk in cond, =", "SELECT * FROM test WHERE k = 2.0 AND e = 1", false, "2,foo2,bar1,1\n", nil},
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
						CREATE INDEX idx_a ON test (a);
						CREATE INDEX idx_b ON test (b);
						CREATE INDEX idx_c ON test (c);
						CREATE INDEX idx_d ON test (d);
					`)
					require.NoError(t, err)
				}

				err = db.Exec("INSERT INTO test (k, a, b, c) VALUES (1, 'foo1', 'bar1', 'baz1')")
				require.NoError(t, err)
				err = db.Exec("INSERT INTO test (k, a, b, e) VALUES (2, 'foo2', 'bar1', 1)")
				require.NoError(t, err)
				err = db.Exec("INSERT INTO test (k, d, e) VALUES (3, 'foo3', 'bar2')")
				require.NoError(t, err)

				st, err := db.Query(test.query, test.params...)
				if test.fails {
					require.Error(t, err)
					return
				}
				require.NoError(t, err)
				defer st.Close()

				var buf bytes.Buffer
				err = record.IteratorToCSV(&buf, st)
				require.NoError(t, err)
				require.Equal(t, test.expected, buf.String())
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
		err = record.IteratorToCSV(&buf, st)
		require.NoError(t, err)
		require.Equal(t, "2,b\n3,c\n4,d\n", buf.String())
	})
}

func TestUpdateStmt(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		fails    bool
		expected string
		params   []interface{}
	}{
		{"No cond", `UPDATE test SET a = 'boo'`, false, "boo,bar1,baz1\nboo,bar2\nfoo3,bar3\n", nil},
		{"No cond / with ident string", `UPDATE test SET "a" = 'boo'`, false, "boo,bar1,baz1\nboo,bar2\nfoo3,bar3\n", nil},
		{"No cond / with multiple idents", `UPDATE test SET a = c`, false, "baz1,bar1,baz1\nNULL,bar2\nfoo3,bar3\n", nil},
		{"No cond / with string", `UPDATE test SET 'a' = 'boo'`, true, "", nil},
		{"With cond", "UPDATE test SET a = 1, b = 2 WHERE a = 'foo2'", false, "foo1,bar1,baz1\n1,2\nfoo3,bar3\n", nil},
		{"Field not found", "UPDATE test SET a = 1, b = 2 WHERE a = f", false, "foo1,bar1,baz1\nfoo2,bar2\nfoo3,bar3\n", nil},
		{"Positional params", "UPDATE test SET a = ?, b = ? WHERE a = ?", false, "a,b,baz1\nfoo2,bar2\nfoo3,bar3\n", []interface{}{"a", "b", "foo1"}},
		{"Named params", "UPDATE test SET a = $a, b = $b WHERE a = $c", false, "a,b,baz1\nfoo2,bar2\nfoo3,bar3\n", []interface{}{sql.Named("b", "b"), sql.Named("a", "a"), sql.Named("c", "foo1")}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, err := genji.New(memoryengine.NewEngine())
			require.NoError(t, err)
			defer db.Close()

			err = db.Exec("CREATE TABLE test")
			require.NoError(t, err)
			err = db.Exec("INSERT INTO test (a, b, c) VALUES ('foo1', 'bar1', 'baz1')")
			require.NoError(t, err)
			err = db.Exec("INSERT INTO test (a, b) VALUES ('foo2', 'bar2')")
			require.NoError(t, err)
			err = db.Exec("INSERT INTO test (d, e) VALUES ('foo3', 'bar3')")
			require.NoError(t, err)

			err = db.Exec(test.query, test.params...)
			if test.fails {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)

			st, err := db.Query("SELECT * FROM test")
			require.NoError(t, err)
			defer st.Close()

			var buf bytes.Buffer
			err = record.IteratorToCSV(&buf, st)
			require.NoError(t, err)
			require.Equal(t, test.expected, buf.String())
		})
	}
}
