package genji

import (
	"testing"

	"github.com/asdine/genji/engine/memory"
	"github.com/stretchr/testify/require"
)

func TestParserUdpate(t *testing.T) {
	tests := []struct {
		name     string
		s        string
		expected statement
		errored  bool
	}{
		{"No cond", "UPDATE test SET a = 1",
			updateStmt{
				tableName: "test",
				pairs: map[string]expr{
					"a": int64Value(1),
				},
			},
			false},
		{"With cond", "UPDATE test SET a = 1, b = 2 WHERE age = 10",
			updateStmt{
				tableName: "test",
				pairs: map[string]expr{
					"a": int64Value(1),
					"b": int64Value(2),
				},
				whereExpr: eq(fieldSelector("age"), int64Value(10)),
			},
			false},
		{"Trailing comma", "UPDATE test SET a = 1, WHERE age = 10", nil, true},
		{"No SET", "UPDATE test WHERE age = 10", nil, true},
		{"No pair", "UPDATE test SET WHERE age = 10", nil, true},
		{"query.Field only", "UPDATE test SET a WHERE age = 10", nil, true},
		{"No value", "UPDATE test SET a = WHERE age = 10", nil, true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			q, err := parseQuery(test.s)
			if test.errored {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Len(t, q.Statements, 1)
			require.EqualValues(t, test.expected, q.Statements[0])
		})
	}
}

func TestUpdateStmt(t *testing.T) {
	db, err := New(memory.NewEngine())
	require.NoError(t, err)
	defer db.Close()

	err = db.Exec("CREATE TABLE test; INSERT INTO test (a) VALUES ('foo')")
	require.NoError(t, err)

	err = db.Exec("")
	require.NoError(t, err)

	st, err := db.Query(`
		UPDATE test SET a = 'bar';
		SELECT * FROM test
	`)
	require.NoError(t, err)
	defer st.Close()

	count, err := st.Count()
	require.NoError(t, err)
	require.Equal(t, 1, count)
}
