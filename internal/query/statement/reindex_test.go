package statement_test

import (
	"testing"

	"github.com/chaisql/chai/internal/testutil"
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
		{"ReIndex read-only", `REINDEX __chai_catalog`, []string{}, false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, tx, cleanup := testutil.NewTestTx(t)
			defer cleanup()

			testutil.MustExec(t, db, tx, `
				CREATE TABLE test1(a TEXT, b TEXT);
				CREATE TABLE test2(a TEXT, b TEXT);

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

			err := testutil.Exec(db, tx, test.query)
			if test.fails {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)

			for _, idxName := range tx.Catalog.ListIndexes("") {
				idx, err := tx.Catalog.GetIndex(tx, idxName)
				require.NoError(t, err)
				info, err := tx.Catalog.GetIndexInfo(idxName)
				require.NoError(t, err)

				shouldBeIndexed := false
				for _, name := range test.expectReIndexed {
					if name == info.IndexName {
						shouldBeIndexed = true
						break
					}
				}

				i := 0
				it, err := idx.Tree.Iterator(nil)
				require.NoError(t, err)
				defer it.Close()

				for it.First(); it.Valid(); it.Next() {
					i++
				}
				require.NoError(t, it.Error())
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
