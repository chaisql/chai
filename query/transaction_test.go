package query_test

import (
	"testing"

	"github.com/genjidb/genji"
	"github.com/stretchr/testify/require"
)

func TestTransactionRun(t *testing.T) {
	tests := []struct {
		name    string
		queries []string
		fails   bool
	}{
		{"Same exec/ Basic", []string{`BEGIN`}, false},
		{"Same exec/ Nested transaction", []string{`BEGIN;BEGIN`}, true},
		{"Same exec/ Begin then commit", []string{`BEGIN;COMMIT`}, false},
		{"Same exec/ Begin then rollback", []string{`BEGIN;ROLLBACK`}, false},
		{"Same exec/ Begin, select, then rollback", []string{`BEGIN;SELECT 1;ROLLBACK`}, false},
		{"Multiple execs/ Begin then rollback", []string{`BEGIN`, `ROLLBACK`}, false},
		{"Multiple execs/ Begin then commit", []string{`BEGIN`, `COMMIT`}, false},
		{"Multiple execs/ Double", []string{`BEGIN`, `COMMIT`, `BEGIN`, `COMMIT`}, false},
		{"Multiple execs/ Begin then begin", []string{`BEGIN`, `BEGIN`}, true},
		{"Multiple execs/ Nested", []string{`BEGIN`, `BEGIN`, `COMMIT`, `COMMIT`}, true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, err := genji.Open(":memory:")
			require.NoError(t, err)
			defer db.Close()
			defer db.Exec("ROLLBACK")

			for _, q := range test.queries {
				err = db.Exec(q)
				if err != nil {
					break
				}
			}
			if test.fails {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
		})
	}
}
