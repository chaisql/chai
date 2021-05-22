package query_test

import (
	"testing"

	"github.com/genjidb/genji/internal/testutil"
	"github.com/stretchr/testify/require"
)

func TestReIndex(t *testing.T) {
	tests := []struct {
		name            string
		query           string
		expectReIndexed []string
		fails           bool
	}{
		{"ReIndex all", `REINDEX`, []string{"idx_test1_a", "idx_test1_b", "idx_test2_a", "idx_test2_b"}, false},
		{"ReIndex table", `REINDEX test2`, []string{"idx_test2_a", "idx_test2_b"}, false},
		{"ReIndex index", `REINDEX idx_test1_a`, []string{"idx_test1_a"}, false},
		{"ReIndex unknown", `REINDEX doesntexist`, []string{}, true},
		{"ReIndex read-only", `REINDEX __genji_tables`, []string{}, false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, tx, cleanup := testutil.NewTestTx(t)
			defer cleanup()

			testutil.MustExec(t, tx, `
				CREATE TABLE test1;
				CREATE TABLE test2;

				CREATE INDEX idx_test1_a ON test1(a);
				CREATE INDEX idx_test1_b ON test1(b);
				CREATE INDEX idx_test2_a ON test2(a);
				CREATE INDEX idx_test2_b ON test2(b);

				INSERT INTO test1(a, b) VALUES (1, 'a'), (2, 'b');
				INSERT INTO test2(a, b) VALUES (3, 'c'), (4, 'd');
			`)

			// truncate all indexes
			c := tx.Catalog
			for _, idxName := range c.ListIndexes("") {
				idx, err := c.GetIndex(tx, idxName)
				require.NoError(t, err)
				err = idx.Truncate()
				require.NoError(t, err)
			}

			err := testutil.Exec(tx, test.query)
			if test.fails {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)

			for _, idxName := range tx.Catalog.ListIndexes("") {
				idx, err := tx.Catalog.GetIndex(tx, idxName)
				require.NoError(t, err)

				shouldBeIndexed := false
				for _, name := range test.expectReIndexed {
					if name == idx.Info.IndexName {
						shouldBeIndexed = true
						break
					}
				}

				i := 0
				err = idx.AscendGreaterOrEqual(nil, func(val []byte, key []byte) error {
					i++
					return nil
				})
				require.NoError(t, err)
				if shouldBeIndexed {
					require.Equal(t, 2, i)
				} else {
					require.Equal(t, 0, i)
				}
			}

			require.NoError(t, err)
		})
	}
}
