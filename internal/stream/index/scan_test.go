package index_test

import (
	"testing"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/internal/database"
	"github.com/genjidb/genji/internal/environment"
	"github.com/genjidb/genji/internal/stream"
	"github.com/genjidb/genji/internal/stream/index"
	"github.com/genjidb/genji/internal/testutil"
	"github.com/genjidb/genji/internal/testutil/assert"
	"github.com/genjidb/genji/types"
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
			require.Equal(t, `index.Scan("idx_test_a", [{"min": [1], "max": [2]}])`, index.Scan("idx_test_a", stream.Range{
				Min: testutil.ExprList(t, `[1]`), Max: testutil.ExprList(t, `[2]`),
			}).String())

			op := index.Scan("idx_test_a", stream.Range{
				Min: testutil.ExprList(t, `[1]`), Max: testutil.ExprList(t, `[2]`),
			})
			op.Reverse = true

			require.Equal(t, `index.ScanReverse("idx_test_a", [{"min": [1], "max": [2]}])`, op.String())
		})

		t.Run("idx_test_a_b", func(t *testing.T) {
			require.Equal(t, `index.Scan("idx_test_a_b", [{"min": [1, 1], "max": [2, 2]}])`, index.Scan("idx_test_a_b", stream.Range{
				Min: testutil.ExprList(t, `[1, 1]`),
				Max: testutil.ExprList(t, `[2, 2]`),
			}).String())

			op := index.Scan("idx_test_a_b", stream.Range{
				Min: testutil.ExprList(t, `[1, 1]`),
				Max: testutil.ExprList(t, `[2, 2]`),
			})
			op.Reverse = true

			require.Equal(t, `index.ScanReverse("idx_test_a_b", [{"min": [1, 1], "max": [2, 2]}])`, op.String())
		})
	})
}

func testIndexScan(t *testing.T, getOp func(db *database.Database, tx *database.Transaction, name string, indexOn string, reverse bool, ranges ...stream.Range) stream.Operator) {
	tests := []struct {
		name                  string
		indexOn               string
		docsInTable, expected testutil.Docs
		ranges                stream.Ranges
		reverse               bool
		fails                 bool
	}{
		{name: "empty", indexOn: "a"},
		{
			"no range", "a",
			testutil.MakeDocuments(t, `{"a": 1}`, `{"a": 2}`),
			testutil.MakeDocuments(t, `{"a": 1}`, `{"a": 2}`),
			nil, false, false,
		},
		{
			"no range", "a, b",
			testutil.MakeDocuments(t, `{"a": 1, "b": 2}`, `{"a": 2, "b": 3}`),
			testutil.MakeDocuments(t, `{"a": 1, "b": 2}`, `{"a": 2, "b": 3}`),
			nil, false, false,
		},
		{
			"max:2", "a",
			testutil.MakeDocuments(t, `{"a": 1}`, `{"a": 2}`),
			testutil.MakeDocuments(t, `{"a": 1}`, `{"a": 2}`),
			stream.Ranges{
				stream.Range{Max: testutil.ExprList(t, `[2]`), Paths: []document.Path{testutil.ParseDocumentPath(t, "a")}},
			},
			false, false,
		},
		{
			"max:1.2", "a",
			testutil.MakeDocuments(t, `{"a": 1}`, `{"a": 2}`),
			testutil.MakeDocuments(t, `{"a": 1}`),
			stream.Ranges{
				stream.Range{Max: testutil.ExprList(t, `[1.2]`), Paths: []document.Path{testutil.ParseDocumentPath(t, "a")}},
			},
			false, false,
		},
		{
			"max:[2, 2]", "a, b",
			testutil.MakeDocuments(t, `{"a": 1, "b": 2}`, `{"a": 2, "b": 2}`),
			testutil.MakeDocuments(t, `{"a": 2, "b": 2}`),
			stream.Ranges{
				stream.Range{Max: testutil.ExprList(t, `[2, 2]`), Paths: testutil.ParseDocumentPaths(t, "a", "b")},
			},
			false, false,
		},
		{
			"max:[2, 2.2]", "a, b",
			testutil.MakeDocuments(t, `{"a": 1, "b": 2}`, `{"a": 2, "b": 2}`),
			testutil.MakeDocuments(t, `{"a": 2, "b": 2}`),
			stream.Ranges{
				stream.Range{Max: testutil.ExprList(t, `[2, 2.2]`), Paths: testutil.ParseDocumentPaths(t, "a", "b")},
			},
			false, false,
		},
		{
			"max:1", "a",
			testutil.MakeDocuments(t, `{"a": 1}`, `{"a": 2}`),
			testutil.MakeDocuments(t, `{"a": 1}`),
			stream.Ranges{
				stream.Range{Max: testutil.ExprList(t, `[1]`), Paths: []document.Path{testutil.ParseDocumentPath(t, "a")}},
			},
			false, false,
		},
		{
			"max:[1, 2]", "a, b",
			testutil.MakeDocuments(t, `{"a": 1, "b": 2}`, `{"a": 2, "b": 2}`),
			testutil.MakeDocuments(t, `{"a": 1, "b": 2}`),
			stream.Ranges{
				stream.Range{Max: testutil.ExprList(t, `[1, 2]`), Paths: testutil.ParseDocumentPaths(t, "a", "b")},
			},
			false, false,
		},
		{
			"max:[1.1, 2]", "a, b",
			testutil.MakeDocuments(t, `{"a": 1, "b": 2}`, `{"a": 2, "b": 2}`),
			testutil.MakeDocuments(t),
			stream.Ranges{
				stream.Range{Max: testutil.ExprList(t, `[1.1, 2]`), Paths: testutil.ParseDocumentPaths(t, "a", "b")},
			},
			false, false,
		},
		{
			"min", "a",
			testutil.MakeDocuments(t, `{"a": 1}`, `{"a": 2}`),
			testutil.MakeDocuments(t, `{"a": 1}`, `{"a": 2}`),
			stream.Ranges{
				stream.Range{Min: testutil.ExprList(t, `[1]`), Paths: []document.Path{testutil.ParseDocumentPath(t, "a")}},
			},
			false, false,
		},
		{
			"min:[1],exclusive", "a",
			testutil.MakeDocuments(t, `{"a": 1}`, `{"a": 2}`),
			testutil.MakeDocuments(t, `{"a": 2}`),
			stream.Ranges{
				stream.Range{Min: testutil.ExprList(t, `[1]`), Paths: []document.Path{testutil.ParseDocumentPath(t, "a")}, Exclusive: true},
			},
			false, false,
		},
		{
			"min:[1],exclusive", "a, b",
			testutil.MakeDocuments(t, `{"a": 1, "b": 1}`, `{"a": 2, "b": 2}`),
			testutil.MakeDocuments(t, `{"a": 2, "b": 2}`),
			stream.Ranges{
				stream.Range{Min: testutil.ExprList(t, `[1]`), Paths: testutil.ParseDocumentPaths(t, "a", "b"), Exclusive: true},
			},
			false, false,
		},
		{
			"min:[2, 1]", "a, b",
			testutil.MakeDocuments(t, `{"a": 1, "b": 2}`, `{"a": 2, "b": 2}`),
			testutil.MakeDocuments(t, `{"a": 2, "b": 2}`),
			stream.Ranges{
				stream.Range{
					Min:   testutil.ExprList(t, `[2, 1]`),
					Paths: testutil.ParseDocumentPaths(t, "a", "b"),
				},
			},
			false, false,
		},
		{
			"min:[2, 1.5]", "a, b",
			testutil.MakeDocuments(t, `{"a": 1, "b": 2}`, `{"a": 2, "b": 2}`),
			testutil.MakeDocuments(t, `{"a": 2, "b": 2}`),
			stream.Ranges{
				stream.Range{
					Min:   testutil.ExprList(t, `[2, 1.5]`),
					Paths: testutil.ParseDocumentPaths(t, "a", "b"),
				},
			},
			false, false,
		},
		{
			"min/max", "a",
			testutil.MakeDocuments(t, `{"a": 1}`, `{"a": 2}`),
			testutil.MakeDocuments(t, `{"a": 1}`, `{"a": 2}`),
			stream.Ranges{
				stream.Range{
					Min:   testutil.ExprList(t, `[1]`),
					Max:   testutil.ExprList(t, `[2]`),
					Paths: []document.Path{testutil.ParseDocumentPath(t, "a")},
				},
			},
			false, false,
		},
		{
			"min:[1, 1], max:[2,2]", "a, b",
			testutil.MakeDocuments(t, `{"a": 1, "b": 2}`, `{"a": 2, "b": 2}`),
			testutil.MakeDocuments(t, `{"a": 1, "b": 2}`, `{"a": 2, "b": 2}`),
			stream.Ranges{
				stream.Range{
					Min:   testutil.ExprList(t, `[1, 1]`),
					Max:   testutil.ExprList(t, `[2, 2]`),
					Paths: testutil.ParseDocumentPaths(t, "a", "b"),
				},
			},
			false, false,
		},
		{
			"min:[1, 1], max:[2,2] bis", "a, b",
			testutil.MakeDocuments(t, `{"a": 1, "b": 3}`, `{"a": 2, "b": 2}`),
			testutil.MakeDocuments(t, `{"a": 1, "b": 3}`, `{"a": 2, "b": 2}`), // [1, 3] < [2, 2]
			stream.Ranges{
				stream.Range{
					Min:   testutil.ExprList(t, `[1, 1]`),
					Max:   testutil.ExprList(t, `[2, 2]`),
					Paths: testutil.ParseDocumentPaths(t, "a", "b"),
				},
			},
			false, false,
		},
		{
			"reverse/no range", "a",
			testutil.MakeDocuments(t, `{"a": 1}`, `{"a": 2}`),
			testutil.MakeDocuments(t, `{"a": 2}`, `{"a": 1}`),
			nil, true, false,
		},
		{
			"reverse/max", "a",
			testutil.MakeDocuments(t, `{"a": 1}`, `{"a": 2}`),
			testutil.MakeDocuments(t, `{"a": 2}`, `{"a": 1}`),
			stream.Ranges{
				stream.Range{Max: testutil.ExprList(t, `[2]`), Paths: []document.Path{testutil.ParseDocumentPath(t, "a")}},
			},
			true, false,
		},
		{
			"reverse/max", "a, b",
			testutil.MakeDocuments(t, `{"a": 1, "b": 1}`, `{"a": 2, "b": 2}`),
			testutil.MakeDocuments(t, `{"a": 2, "b": 2}`),
			stream.Ranges{
				stream.Range{
					Max:   testutil.ExprList(t, `[2, 2]`),
					Paths: testutil.ParseDocumentPaths(t, "a", "b"),
				},
			},
			true, false,
		},
		{
			"reverse/min", "a",
			testutil.MakeDocuments(t, `{"a": 1}`, `{"a": 2}`),
			testutil.MakeDocuments(t, `{"a": 2}`, `{"a": 1}`),
			stream.Ranges{
				stream.Range{Min: testutil.ExprList(t, `[1]`), Paths: []document.Path{testutil.ParseDocumentPath(t, "a")}},
			},
			true, false,
		},
		{
			"reverse/min neg", "a",
			testutil.MakeDocuments(t, `{"a": 1}`, `{"a": -2}`),
			testutil.MakeDocuments(t, `{"a": 1}`),
			stream.Ranges{
				stream.Range{Min: testutil.ExprList(t, `[1]`), Paths: []document.Path{testutil.ParseDocumentPath(t, "a")}},
			},
			true, false,
		},
		{
			"reverse/min", "a, b",
			testutil.MakeDocuments(t, `{"a": 1, "b": 1}`, `{"a": 2, "b": 2}`),
			testutil.MakeDocuments(t, `{"a": 1, "b": 1}`),
			stream.Ranges{
				stream.Range{
					Min:   testutil.ExprList(t, `[1, 1]`),
					Paths: testutil.ParseDocumentPaths(t, "a", "b"),
				},
			},
			true, false,
		},
		{
			"reverse/min/max", "a",
			testutil.MakeDocuments(t, `{"a": 1}`, `{"a": 2}`),
			testutil.MakeDocuments(t, `{"a": 2}`, `{"a": 1}`),
			stream.Ranges{
				stream.Range{
					Min:   testutil.ExprList(t, `[1]`),
					Max:   testutil.ExprList(t, `[2]`),
					Paths: []document.Path{testutil.ParseDocumentPath(t, "a")},
				},
			},
			true, false,
		},
		{
			"reverse/min/max", "a, b",
			testutil.MakeDocuments(t, `{"a": 1, "b": 1}`, `{"a": 2, "b": 2}`),
			testutil.MakeDocuments(t, `{"a": 2, "b": 2}`, `{"a": 1, "b": 1}`),
			stream.Ranges{
				stream.Range{
					Min:   testutil.ExprList(t, `[1, 1]`),
					Max:   testutil.ExprList(t, `[2, 2]`),
					Paths: testutil.ParseDocumentPaths(t, "a", "b"),
				},
			},
			true, false,
		},
		{
			"max:[1]", "a, b",
			testutil.MakeDocuments(t, `{"a": 1, "b": 1}`, `{"a": 2, "b": 2}`, `{"a": 1, "b": 9223372036854775807}`),
			testutil.MakeDocuments(t, `{"a": 1, "b": 1}`, `{"a": 1, "b": 9223372036854775807}`),
			stream.Ranges{
				stream.Range{
					Max:   testutil.ExprList(t, `[1]`),
					Paths: testutil.ParseDocumentPaths(t, "a", "b"),
				},
			},
			false, false,
		},
		{
			"reverse max:[1]", "a, b",
			testutil.MakeDocuments(t, `{"a": 1, "b": 1}`, `{"a": 2, "b": 2}`, `{"a": 1, "b": 9223372036854775807}`),
			testutil.MakeDocuments(t, `{"a": 1, "b": 9223372036854775807}`, `{"a": 1, "b": 1}`),
			stream.Ranges{
				stream.Range{
					Max:       testutil.ExprList(t, `[1]`),
					Exclusive: false,
					Exact:     false,
					Paths:     testutil.ParseDocumentPaths(t, "a", "b"),
				},
			},
			true, false,
		},
		{
			"max:[1, 2]", "a, b, c",
			testutil.MakeDocuments(t, `{"a": 1, "b": 2, "c": 1}`, `{"a": 2, "b": 2, "c":  2}`, `{"a": 1, "b": 2, "c": 9223372036854775807}`),
			testutil.MakeDocuments(t, `{"a": 1, "b": 2, "c": 1}`, `{"a": 1, "b": 2, "c": 9223372036854775807}`),
			stream.Ranges{
				stream.Range{
					Max: testutil.ExprList(t, `[1, 2]`), Paths: testutil.ParseDocumentPaths(t, "a", "b", "c"),
				},
			},
			false, false,
		},
		{
			"min:[1]", "a, b",
			testutil.MakeDocuments(t, `{"a": 1, "b": -2}`, `{"a": -2, "b": 2}`, `{"a": 1, "b": 1}`),
			testutil.MakeDocuments(t, `{"a": 1, "b": -2}`, `{"a": 1, "b": 1}`),
			stream.Ranges{
				stream.Range{Min: testutil.ExprList(t, `[1]`), Paths: testutil.ParseDocumentPaths(t, "a", "b")},
			},
			false, false,
		},
		{
			"min:[1]", "a, b, c",
			testutil.MakeDocuments(t, `{"a": 1, "b": -2, "c": 0}`, `{"a": -2, "b": 2, "c": 1}`, `{"a": 1, "b": 1, "c": 2}`),
			testutil.MakeDocuments(t, `{"a": 1, "b": -2, "c": 0}`, `{"a": 1, "b": 1, "c": 2}`),
			stream.Ranges{
				stream.Range{Min: testutil.ExprList(t, `[1]`), Paths: testutil.ParseDocumentPaths(t, "a", "b", "c")},
			},
			false, false,
		},
		{
			"reverse min:[1]", "a, b",
			testutil.MakeDocuments(t, `{"a": 1, "b": -2}`, `{"a": -2, "b": 2}`, `{"a": 1, "b": 1}`),
			testutil.MakeDocuments(t, `{"a": 1, "b": 1}`, `{"a": 1, "b": -2}`),
			stream.Ranges{
				stream.Range{Min: testutil.ExprList(t, `[1]`), Paths: testutil.ParseDocumentPaths(t, "a", "b")},
			},
			true, false,
		},
		{
			"min:[1], max[2]", "a, b",
			testutil.MakeDocuments(t, `{"a": 1, "b": -2}`, `{"a": -2, "b": 2}`, `{"a": 2, "b": 42}`, `{"a": 3, "b": -1}`),
			testutil.MakeDocuments(t, `{"a": 1, "b": -2}`, `{"a": 2, "b": 42}`),
			stream.Ranges{
				stream.Range{
					Min:   testutil.ExprList(t, `[1]`),
					Max:   testutil.ExprList(t, `[2]`),
					Paths: testutil.ParseDocumentPaths(t, "a", "b"),
				},
			},
			false, false,
		},
		{
			"reverse min:[1], max[2]", "a, b",
			testutil.MakeDocuments(t, `{"a": 1, "b": -2}`, `{"a": -2, "b": 2}`, `{"a": 2, "b": 42}`, `{"a": 3, "b": -1}`),
			testutil.MakeDocuments(t, `{"a": 2, "b": 42}`, `{"a": 1, "b": -2}`),
			stream.Ranges{
				stream.Range{
					Min:   testutil.ExprList(t, `[1]`),
					Max:   testutil.ExprList(t, `[2]`),
					Paths: testutil.ParseDocumentPaths(t, "a", "b"),
				},
			},
			true, false,
		},
	}

	for _, test := range tests {
		t.Run(test.name+":index on "+test.indexOn, func(t *testing.T) {
			db, tx, cleanup := testutil.NewTestTx(t)
			defer cleanup()

			testutil.MustExec(t, db, tx, "CREATE TABLE test (a INTEGER, b INTEGER, c INTEGER);")

			for _, doc := range test.docsInTable {
				testutil.MustExec(t, db, tx, "INSERT INTO test VALUES ?", environment.Param{Value: doc})
			}

			op := getOp(db, tx, "idx_test_a", test.indexOn, test.reverse, test.ranges...)
			var env environment.Environment
			env.Tx = tx
			env.Catalog = db.Catalog
			env.DB = db
			env.Params = []environment.Param{{Name: "foo", Value: 1}}

			var i int
			var got testutil.Docs
			err := op.Iterate(&env, func(env *environment.Environment) error {
				d, ok := env.GetDocument()
				require.True(t, ok)
				var fb document.FieldBuffer

				err := fb.Copy(d)
				assert.NoError(t, err)

				got = append(got, &fb)
				v, err := env.GetParamByName("foo")
				assert.NoError(t, err)
				require.Equal(t, types.NewIntegerValue(1), v)
				i++
				return nil
			})
			if test.fails {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				require.Equal(t, len(test.expected), i)
				test.expected.RequireEqual(t, got)
			}
		})
	}
}
