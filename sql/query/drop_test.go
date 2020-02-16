package query_test

import (
	"testing"

	"github.com/asdine/genji"
	"github.com/stretchr/testify/require"
)

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
			db, err := genji.Open(":memory:")
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
