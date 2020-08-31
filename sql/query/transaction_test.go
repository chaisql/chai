package query_test

import (
	"testing"

	"github.com/genjidb/genji"
	"github.com/stretchr/testify/require"
)

func TestTransaction(t *testing.T) {
	tests := []struct {
		name  string
		query string
		fails bool
	}{
		{"Basic", `BEGIN`, false},
		{"Nested transaction", `BEGIN;BEGIN`, true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, err := genji.Open(":memory:")
			require.NoError(t, err)
			defer db.Close()

			err = db.Exec(test.query)
			if test.fails {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
		})
	}
}
