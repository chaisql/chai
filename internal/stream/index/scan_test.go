package index_test

import (
	"testing"

	"github.com/chaisql/chai/internal/database"
	"github.com/chaisql/chai/internal/environment"
	"github.com/chaisql/chai/internal/row"
	"github.com/chaisql/chai/internal/stream"
	"github.com/chaisql/chai/internal/stream/index"
	"github.com/chaisql/chai/internal/testutil"
	"github.com/chaisql/chai/internal/types"
	"github.com/stretchr/testify/require"
)

func TestIndexScan(t *testing.T) {
	testIndexScan(t, func(db *database.Database, tx *database.Transaction, name string, indexOn string, reverse bool, ranges ...stream.Range) stream.Operator {
		t.Helper()

		testutil.MustExec(t, db, tx, "CREATE INDEX idx_test_a ON test("+indexOn+")")

		op := index.Scan(name, ranges...)
		op.Reverse = reverse
		return op
	})

	t.Run("String", func(t *testing.T) {
		t.Run("idx_test_a", func(t *testing.T) {
			require.Equal(t, `index.Scan("idx_test_a", [{"min": (1), "max": (2)}])`, index.Scan("idx_test_a", stream.Range{
				Min: testutil.ExprList(t, `(1)`), Max: testutil.ExprList(t, `(2)`),
			}).String())

			op := index.Scan("idx_test_a", stream.Range{
				Min: testutil.ExprList(t, `(1)`), Max: testutil.ExprList(t, `(2)`),
			})
			op.Reverse = true

			require.Equal(t, `index.ScanReverse("idx_test_a", [{"min": (1), "max": (2)}])`, op.String())
		})

		t.Run("idx_test_a_b", func(t *testing.T) {
			require.Equal(t, `index.Scan("idx_test_a_b", [{"min": (1, 1), "max": (2, 2)}])`, index.Scan("idx_test_a_b", stream.Range{
				Min: testutil.ExprList(t, `(1, 1)`),
				Max: testutil.ExprList(t, `(2, 2)`),
			}).String())

			op := index.Scan("idx_test_a_b", stream.Range{
				Min: testutil.ExprList(t, `(1, 1)`),
				Max: testutil.ExprList(t, `(2, 2)`),
			})
			op.Reverse = true

			require.Equal(t, `index.ScanReverse("idx_test_a_b", [{"min": (1, 1), "max": (2, 2)}])`, op.String())
		})
	})
}

func testIndexScan(t *testing.T, getOp func(db *database.Database, tx *database.Transaction, name string, indexOn string, reverse bool, ranges ...stream.Range) stream.Operator) {
	tests := []struct {
		name                  string
		indexOn               string
		docsInTable, expected testutil.Rows
		ranges                stream.Ranges
		reverse               bool
		fails                 bool
	}{
		{name: "empty", indexOn: "a"},
		{
			"no range", "a",
			testutil.MakeRows(t, `{"a": 1}`, `{"a": 2}`),
			testutil.MakeRows(t, `{"a": 1, "b": null, "c": null}`, `{"a": 2, "b": null, "c": null}`),
			nil, false, false,
		},
		{
			"no range", "a, b",
			testutil.MakeRows(t, `{"a": 1, "b": 2}`, `{"a": 2, "b": 3}`),
			testutil.MakeRows(t, `{"a": 1, "b": 2, "c": null}`, `{"a": 2, "b": 3, "c": null}`),
			nil, false, false,
		},
		{
			"max:2", "a",
			testutil.MakeRows(t, `{"a": 1}`, `{"a": 2}`),
			testutil.MakeRows(t, `{"a": 1, "b": null, "c": null}`, `{"a": 2, "b": null, "c": null}`),
			stream.Ranges{
				stream.Range{Max: testutil.ExprList(t, `(2)`), Columns: []string{"a"}},
			},
			false, false,
		},
		{
			"max:1.2", "a",
			testutil.MakeRows(t, `{"a": 1}`, `{"a": 2}`),
			nil,
			stream.Ranges{
				stream.Range{Max: testutil.ExprList(t, `(1.2)`), Columns: []string{"a"}},
			},
			false, false,
		},
		{
			"max:(2, 2)", "a, b",
			testutil.MakeRows(t, `{"a": 1, "b": 2}`, `{"a": 2, "b": 2}`),
			testutil.MakeRows(t, `{"a": 2, "b": 2, "c": null}`),
			stream.Ranges{
				stream.Range{Max: testutil.ExprList(t, `(2, 2)`), Columns: []string{"a", "b"}},
			},
			false, false,
		},
		{
			"max:(2, 2.2)", "a, b",
			testutil.MakeRows(t, `{"a": 1, "b": 2}`, `{"a": 2, "b": 2}`),
			nil,
			stream.Ranges{
				stream.Range{Max: testutil.ExprList(t, `(2, 2.2)`), Columns: []string{"a", "b"}},
			},
			false, false,
		},
		{
			"max:1", "a",
			testutil.MakeRows(t, `{"a": 1}`, `{"a": 2}`),
			testutil.MakeRows(t, `{"a": 1, "b": null, "c": null}`),
			stream.Ranges{
				stream.Range{Max: testutil.ExprList(t, `(1)`), Columns: []string{"a"}},
			},
			false, false,
		},
		{
			"max:(1, 2)", "a, b",
			testutil.MakeRows(t, `{"a": 1, "b": 2}`, `{"a": 2, "b": 2}`),
			testutil.MakeRows(t, `{"a": 1, "b": 2, "c": null}`),
			stream.Ranges{
				stream.Range{Max: testutil.ExprList(t, `(1, 2)`), Columns: []string{"a", "b"}},
			},
			false, false,
		},
		{
			"max:(1.1, 2)", "a, b",
			testutil.MakeRows(t, `{"a": 1, "b": 2, "c": null}`, `{"a": 2, "b": 2, "c": null}`),
			nil,
			stream.Ranges{
				stream.Range{Max: testutil.ExprList(t, `(1.1, 2)`), Columns: []string{"a", "b"}},
			},
			false, false,
		},
		{
			"min", "a",
			testutil.MakeRows(t, `{"a": 1}`, `{"a": 2}`),
			testutil.MakeRows(t, `{"a": 1, "b": null, "c": null}`, `{"a": 2, "b": null, "c": null}`),
			stream.Ranges{
				stream.Range{Min: testutil.ExprList(t, `(1)`), Columns: []string{"a"}},
			},
			false, false,
		},
		{
			"min:(1),exclusive", "a",
			testutil.MakeRows(t, `{"a": 1}`, `{"a": 2}`),
			testutil.MakeRows(t, `{"a": 2, "b": null, "c": null}`),
			stream.Ranges{
				stream.Range{Min: testutil.ExprList(t, `(1)`), Columns: []string{"a"}, Exclusive: true},
			},
			false, false,
		},
		{
			"min:(1),exclusive", "a, b",
			testutil.MakeRows(t, `{"a": 1, "b": 1}`, `{"a": 2, "b": 2}`),
			testutil.MakeRows(t, `{"a": 2, "b": 2, "c": null}`),
			stream.Ranges{
				stream.Range{Min: testutil.ExprList(t, `(1)`), Columns: []string{"a", "b"}, Exclusive: true},
			},
			false, false,
		},
		{
			"min:(2, 1)", "a, b",
			testutil.MakeRows(t, `{"a": 1, "b": 2}`, `{"a": 2, "b": 2}`),
			testutil.MakeRows(t, `{"a": 2, "b": 2, "c": null}`),
			stream.Ranges{
				stream.Range{
					Min:     testutil.ExprList(t, `(2, 1)`),
					Columns: []string{"a", "b"},
				},
			},
			false, false,
		},
		{
			"min:(2, 1.5)", "a, b",
			testutil.MakeRows(t, `{"a": 1, "b": 2, "c": null}`, `{"a": 2, "b": 2, "c": null}`),
			nil,
			stream.Ranges{
				stream.Range{
					Min:     testutil.ExprList(t, `(2, 1.5)`),
					Columns: []string{"a", "b"},
				},
			},
			false, false,
		},
		{
			"min/max", "a",
			testutil.MakeRows(t, `{"a": 1}`, `{"a": 2}`),
			testutil.MakeRows(t, `{"a": 1, "b": null, "c": null}`, `{"a": 2, "b": null, "c": null}`),
			stream.Ranges{
				stream.Range{
					Min:     testutil.ExprList(t, `(1)`),
					Max:     testutil.ExprList(t, `(2)`),
					Columns: []string{"a"},
				},
			},
			false, false,
		},
		{
			"min:(1, 1), max:[2,2]", "a, b",
			testutil.MakeRows(t, `{"a": 1, "b": 2}`, `{"a": 2, "b": 2}`),
			testutil.MakeRows(t, `{"a": 1, "b": 2, "c": null}`, `{"a": 2, "b": 2, "c": null}`),
			stream.Ranges{
				stream.Range{
					Min:     testutil.ExprList(t, `(1, 1)`),
					Max:     testutil.ExprList(t, `(2, 2)`),
					Columns: []string{"a", "b"},
				},
			},
			false, false,
		},
		{
			"min:(1, 1), max:[2,2] bis", "a, b",
			testutil.MakeRows(t, `{"a": 1, "b": 3}`, `{"a": 2, "b": 2}`),
			testutil.MakeRows(t, `{"a": 1, "b": 3, "c": null}`, `{"a": 2, "b": 2, "c": null}`), // [1, 3] < (2, 2)
			stream.Ranges{
				stream.Range{
					Min:     testutil.ExprList(t, `(1, 1)`),
					Max:     testutil.ExprList(t, `(2, 2)`),
					Columns: []string{"a", "b"},
				},
			},
			false, false,
		},
		{
			"reverse/no range", "a",
			testutil.MakeRows(t, `{"a": 1}`, `{"a": 2}`),
			testutil.MakeRows(t, `{"a": 2, "b": null, "c": null}`, `{"a": 1, "b": null, "c": null}`),
			nil, true, false,
		},
		{
			"reverse/max", "a",
			testutil.MakeRows(t, `{"a": 1}`, `{"a": 2}`),
			testutil.MakeRows(t, `{"a": 2, "b": null, "c": null}`, `{"a": 1, "b": null, "c": null}`),
			stream.Ranges{
				stream.Range{Max: testutil.ExprList(t, `(2)`), Columns: []string{"a"}},
			},
			true, false,
		},
		{
			"reverse/max", "a, b",
			testutil.MakeRows(t, `{"a": 1, "b": 1}`, `{"a": 2, "b": 2}`),
			testutil.MakeRows(t, `{"a": 2, "b": 2, "c": null}`),
			stream.Ranges{
				stream.Range{
					Max:     testutil.ExprList(t, `(2, 2)`),
					Columns: []string{"a", "b"},
				},
			},
			true, false,
		},
		{
			"reverse/min", "a",
			testutil.MakeRows(t, `{"a": 1}`, `{"a": 2}`),
			testutil.MakeRows(t, `{"a": 2, "b": null, "c": null}`, `{"a": 1, "b": null, "c": null}`),
			stream.Ranges{
				stream.Range{Min: testutil.ExprList(t, `(1)`), Columns: []string{"a"}},
			},
			true, false,
		},
		{
			"reverse/min neg", "a",
			testutil.MakeRows(t, `{"a": 1}`, `{"a": -2}`),
			testutil.MakeRows(t, `{"a": 1, "b": null, "c": null}`),
			stream.Ranges{
				stream.Range{Min: testutil.ExprList(t, `(1)`), Columns: []string{"a"}},
			},
			true, false,
		},
		{
			"reverse/min", "a, b",
			testutil.MakeRows(t, `{"a": 1, "b": 1}`, `{"a": 2, "b": 2}`),
			testutil.MakeRows(t, `{"a": 1, "b": 1, "c": null}`),
			stream.Ranges{
				stream.Range{
					Min:     testutil.ExprList(t, `(1, 1)`),
					Columns: []string{"a", "b"},
				},
			},
			true, false,
		},
		{
			"reverse/min/max", "a",
			testutil.MakeRows(t, `{"a": 1}`, `{"a": 2}`),
			testutil.MakeRows(t, `{"a": 2, "b": null, "c": null}`, `{"a": 1, "b": null, "c": null}`),
			stream.Ranges{
				stream.Range{
					Min:     testutil.ExprList(t, `(1)`),
					Max:     testutil.ExprList(t, `(2)`),
					Columns: []string{"a"},
				},
			},
			true, false,
		},
		{
			"reverse/min/max", "a, b",
			testutil.MakeRows(t, `{"a": 1, "b": 1}`, `{"a": 2, "b": 2}`),
			testutil.MakeRows(t, `{"a": 2, "b": 2, "c": null}`, `{"a": 1, "b": 1, "c": null}`),
			stream.Ranges{
				stream.Range{
					Min:     testutil.ExprList(t, `(1, 1)`),
					Max:     testutil.ExprList(t, `(2, 2)`),
					Columns: []string{"a", "b"},
				},
			},
			true, false,
		},
		{
			"max:(1)", "a, b",
			testutil.MakeRows(t, `{"a": 1, "b": 1}`, `{"a": 2, "b": 2}`, `{"a": 1, "b": 9223372036854775807}`),
			testutil.MakeRows(t, `{"a": 1, "b": 1, "c": null}`, `{"a": 1, "b": 9223372036854775807, "c": null}`),
			stream.Ranges{
				stream.Range{
					Max:     testutil.ExprList(t, `(1)`),
					Columns: []string{"a", "b"},
				},
			},
			false, false,
		},
		{
			"reverse max:(1)", "a, b",
			testutil.MakeRows(t, `{"a": 1, "b": 1}`, `{"a": 2, "b": 2}`, `{"a": 1, "b": 9223372036854775807}`),
			testutil.MakeRows(t, `{"a": 1, "b": 9223372036854775807, "c": null}`, `{"a": 1, "b": 1, "c": null}`),
			stream.Ranges{
				stream.Range{
					Max:       testutil.ExprList(t, `(1)`),
					Exclusive: false,
					Exact:     false,
					Columns:   []string{"a", "b"},
				},
			},
			true, false,
		},
		{
			"max:(1, 2)", "a, b, c",
			testutil.MakeRows(t, `{"a": 1, "b": 2, "c": 1}`, `{"a": 2, "b": 2, "c":  2}`, `{"a": 1, "b": 2, "c": 9223372036854775807}`),
			testutil.MakeRows(t, `{"a": 1, "b": 2, "c": 1}`, `{"a": 1, "b": 2, "c": 9223372036854775807}`),
			stream.Ranges{
				stream.Range{
					Max: testutil.ExprList(t, `(1, 2)`), Columns: []string{"a", "b", "c"},
				},
			},
			false, false,
		},
		{
			"min:(1)", "a, b",
			testutil.MakeRows(t, `{"a": 1, "b": -2}`, `{"a": -2, "b": 2}`, `{"a": 1, "b": 1}`),
			testutil.MakeRows(t, `{"a": 1, "b": -2, "c": null}`, `{"a": 1, "b": 1, "c": null}`),
			stream.Ranges{
				stream.Range{Min: testutil.ExprList(t, `(1)`), Columns: []string{"a", "b"}},
			},
			false, false,
		},
		{
			"min:(1)", "a, b, c",
			testutil.MakeRows(t, `{"a": 1, "b": -2, "c": 0}`, `{"a": -2, "b": 2, "c": 1}`, `{"a": 1, "b": 1, "c": 2}`),
			testutil.MakeRows(t, `{"a": 1, "b": -2, "c": 0}`, `{"a": 1, "b": 1, "c": 2}`),
			stream.Ranges{
				stream.Range{Min: testutil.ExprList(t, `(1)`), Columns: []string{"a", "b", "c"}},
			},
			false, false,
		},
		{
			"reverse min:(1)", "a, b",
			testutil.MakeRows(t, `{"a": 1, "b": -2}`, `{"a": -2, "b": 2}`, `{"a": 1, "b": 1}`),
			testutil.MakeRows(t, `{"a": 1, "b": 1, "c": null}`, `{"a": 1, "b": -2, "c": null}`),
			stream.Ranges{
				stream.Range{Min: testutil.ExprList(t, `(1)`), Columns: []string{"a", "b"}},
			},
			true, false,
		},
		{
			"min:(1), max(2)", "a, b",
			testutil.MakeRows(t, `{"a": 1, "b": -2}`, `{"a": -2, "b": 2}`, `{"a": 2, "b": 42}`, `{"a": 3, "b": -1}`),
			testutil.MakeRows(t, `{"a": 1, "b": -2, "c": null}`, `{"a": 2, "b": 42, "c": null}`),
			stream.Ranges{
				stream.Range{
					Min:     testutil.ExprList(t, `(1)`),
					Max:     testutil.ExprList(t, `(2)`),
					Columns: []string{"a", "b"},
				},
			},
			false, false,
		},
		{
			"reverse min:(1), max(2)", "a, b",
			testutil.MakeRows(t, `{"a": 1, "b": -2}`, `{"a": -2, "b": 2}`, `{"a": 2, "b": 42}`, `{"a": 3, "b": -1}`),
			testutil.MakeRows(t, `{"a": 2, "b": 42, "c": null}`, `{"a": 1, "b": -2, "c": null}`),
			stream.Ranges{
				stream.Range{
					Min:     testutil.ExprList(t, `(1)`),
					Max:     testutil.ExprList(t, `(2)`),
					Columns: []string{"a", "b"},
				},
			},
			true, false,
		},
	}

	for _, test := range tests {
		t.Run(test.name+":index on "+test.indexOn, func(t *testing.T) {
			db, tx, cleanup := testutil.NewTestTx(t)
			defer cleanup()

			testutil.MustExec(t, db, tx, "CREATE TABLE test (a BIGINT, b BIGINT, c BIGINT);")

			for _, r := range test.docsInTable {
				var a, b, c *int64
				err := row.Scan(r, &a, &b, &c)
				require.NoError(t, err)
				testutil.MustExec(t, db, tx, "INSERT INTO test VALUES (?, ?, ?)", environment.Param{Value: a}, environment.Param{Value: b}, environment.Param{Value: c})
			}

			op := getOp(db, tx, "idx_test_a", test.indexOn, test.reverse, test.ranges...)
			var env environment.Environment
			env.Tx = tx
			env.DB = db
			env.Params = []environment.Param{{Name: "foo", Value: 1}}

			var i int
			var got testutil.Rows
			err := op.Iterate(&env, func(env *environment.Environment) error {
				r, ok := env.GetRow()
				require.True(t, ok)
				var fb row.ColumnBuffer

				err := fb.Copy(r)
				require.NoError(t, err)

				got = append(got, &fb)
				v, err := env.GetParamByName("foo")
				require.NoError(t, err)
				require.Equal(t, types.NewBigintValue(1), v)
				i++
				return nil
			})
			if test.fails {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, len(test.expected), i)
				test.expected.RequireEqual(t, got)
			}
		})
	}
}
