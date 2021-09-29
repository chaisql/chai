package stream_test

import (
	"testing"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/internal/environment"
	"github.com/genjidb/genji/internal/stream"
	"github.com/genjidb/genji/internal/testutil"
	"github.com/genjidb/genji/internal/testutil/assert"
	"github.com/stretchr/testify/require"
)

func TestUnion(t *testing.T) {
	tests := []struct {
		name                           string
		first, second, third, expected testutil.Docs
		fails                          bool
	}{
		{
			"same docs",
			testutil.MakeDocuments(t, `{"a": 1, "b": 1}`, `{"a": 2, "b": 2}`),
			testutil.MakeDocuments(t, `{"a": 1, "b": 1}`, `{"a": 2, "b": 2}`),
			testutil.MakeDocuments(t, `{"a": 1, "b": 1}`, `{"a": 2, "b": 2}`),
			testutil.MakeDocuments(t, `{"a": 1, "b": 1}`, `{"a": 2, "b": 2}`),
			false,
		},
		{
			"different docs",
			testutil.MakeDocuments(t, `{"a": 1, "b": 1}`, `{"a": 1, "b": 2}`),
			testutil.MakeDocuments(t, `{"a": 2, "b": 1}`, `{"a": 2, "b": 2}`),
			testutil.MakeDocuments(t, `{"a": 3, "b": 1}`, `{"a": 3, "b": 2}`),
			testutil.MakeDocuments(t, `{"a": 1, "b": 1}`, `{"a": 1, "b": 2}`, `{"a": 2, "b": 1}`, `{"a": 2, "b": 2}`, `{"a": 3, "b": 1}`, `{"a": 3, "b": 2}`),
			false,
		},
		{
			"mixed",
			testutil.MakeDocuments(t, `{"a": 1}`, `{"a": 1}`, `{"a": 2}`),
			testutil.MakeDocuments(t, `{"a": 1}`, `{"a": 1}`, `{"a": 2}`),
			testutil.MakeDocuments(t, `{"a": 1}`, `{"a": 1}`, `{"a": 2}`),
			testutil.MakeDocuments(t, `{"a": 1}`, `{"a": 2}`),
			false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, tx, cleanup := testutil.NewTestTx(t)
			defer cleanup()

			st := stream.New(stream.Union(
				stream.Documents(test.first...),
				stream.Documents(test.second...),
				stream.Documents(test.third...),
			))
			var env environment.Environment
			env.Tx = tx
			env.DB = db
			env.Catalog = db.Catalog

			var i int
			var got testutil.Docs
			err := st.Iterate(&env, func(env *environment.Environment) error {
				d, ok := env.GetDocument()
				require.True(t, ok)
				var fb document.FieldBuffer

				err := fb.Copy(d)
				assert.NoError(t, err)

				got = append(got, &fb)
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

	t.Run("String", func(t *testing.T) {
		st := stream.New(stream.Union(
			stream.Documents(testutil.MakeDocuments(t, `{"a": 1}`, `{"a": 2}`)...),
			stream.Documents(testutil.MakeDocuments(t, `{"a": 3}`, `{"a": 4}`)...),
			stream.Documents(testutil.MakeDocuments(t, `{"a": 5}`, `{"a": 6}`)...),
		))

		require.Equal(t, `union(docs({"a": 1}, {"a": 2}), docs({"a": 3}, {"a": 4}), docs({"a": 5}, {"a": 6}))`, st.String())
	})
}
