package query_test

import (
	"testing"

	"github.com/genjidb/genji"
	"github.com/genjidb/genji/document"
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
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, err := genji.Open(":memory:")
			require.NoError(t, err)
			defer db.Close()

			err = db.Exec(`
				CREATE TABLE test1;
				CREATE TABLE test2;

				INSERT INTO test1(a, b) VALUES (1, 'a'), (2, 'b');
				INSERT INTO test2(a, b) VALUES (3, 'c'), (4, 'd');

				CREATE INDEX idx_test1_a ON test1(a);
				CREATE INDEX idx_test1_b ON test1(b);
				CREATE INDEX idx_test2_a ON test2(a);
				CREATE INDEX idx_test2_b ON test2(b);
			`)
			require.NoError(t, err)

			err = db.Exec(test.query)
			if test.fails {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)

			err = db.View(func(tx *genji.Tx) error {
				idxList, err := tx.ListIndexes()
				require.NoError(t, err)

				for _, cfg := range idxList {
					idx, err := tx.GetIndex(cfg.IndexName)
					require.NoError(t, err)

					shouldBeIndexed := false
					for _, name := range test.expectReIndexed {
						if name == idx.Opts.IndexName {
							shouldBeIndexed = true
							break
						}
					}

					i := 0
					err = idx.AscendGreaterOrEqual(document.Value{}, func(val []byte, key []byte, isEqual bool) error {
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

				return nil
			})
			require.NoError(t, err)
		})
	}

}
