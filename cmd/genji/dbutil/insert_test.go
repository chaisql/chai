package dbutil

import (
	"bytes"
	"strings"
	"testing"

	"github.com/genjidb/genji"
	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/internal/testutil"
	"github.com/genjidb/genji/internal/testutil/assert"
	"github.com/genjidb/genji/types"
	"github.com/stretchr/testify/require"
)

func TestInsertJSON(t *testing.T) {
	tests := []struct {
		name  string
		data  string
		want  string
		fails bool
	}{
		{"Simple Json", `{"a": 1}`, `[{"a": 1}]`, false},
		{"JSON object", `{"a": {"b": [1, 2, 3]}}`, `[{"a": {"b": [1, 2, 3]}}]`, false},
		{"nested document", `{"a": {"b": [1, 2, 3]}}`, `[{"a": {"b": [1, 2, 3]}}]`, false},
		{"nested array multiple indexes", `{"a": {"b": [1, 2, [1, 2, {"c": "foo"}]]}}`, `[{"a": {"b": [1, 2, [1, 2, {"c": "foo"}]]}}]`, false},
		{"document in array", `{"a": [{"b":"foo"}, 2, 3]}`, `[{"a": [{"b":"foo"}, 2, 3]}]`, false},
		{"Non closed json array", `[{"foo":"bar"}`, ``, true},
		{"Non closed json stream", `{"foo":"bar"`, ``, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, err := genji.Open(":memory:")
			assert.NoError(t, err)
			defer db.Close()

			err = db.Exec(`CREATE TABLE foo`)
			assert.NoError(t, err)
			err = InsertJSON(db, "foo", strings.NewReader(tt.data))
			if tt.fails {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)
			res, err := db.Query("SELECT * FROM foo")
			defer res.Close()
			assert.NoError(t, err)

			var buf bytes.Buffer
			err = testutil.IteratorToJSONArray(&buf, res)
			assert.NoError(t, err)
			require.JSONEq(t, tt.want, buf.String())
		})
	}

	t.Run(`Json Array`, func(t *testing.T) {
		const jsonArray = `
	[
		{"Name": "Ed", "Text": "Knock knock."},
		{"Name": "Sam", "Text": "Who's there?"},
		{"Name": "Ed", "Text": "Go fmt."},
		{"Name": "Sam", "Text": "Go fmt who?"},
		{"Name": "Ed", "Text": "Go fmt yourself!"}
	]
`
		jsonStreamResult := []string{`{"Name": "Ed", "Text": "Knock knock."}`,
			`{"Name": "Sam", "Text": "Who's there?"}`, `{"Name": "Ed", "Text": "Go fmt."}`,
			`{"Name": "Sam", "Text": "Go fmt who?"}`,
			`{"Name": "Ed", "Text": "Go fmt yourself!"}`}

		db, err := genji.Open(":memory:")
		assert.NoError(t, err)
		defer db.Close()

		err = db.Exec(`CREATE TABLE foo`)
		assert.NoError(t, err)
		err = InsertJSON(db, "foo", strings.NewReader(jsonArray))
		assert.NoError(t, err)
		res, err := db.Query("SELECT * FROM foo")
		defer res.Close()
		assert.NoError(t, err)

		i := 0
		_ = res.Iterate(func(d types.Document) error {
			data, err := document.MarshalJSON(d)
			assert.NoError(t, err)
			require.JSONEq(t, jsonStreamResult[i], string(data))
			i++
			return nil
		})
	})

	t.Run(`Json Stream`, func(t *testing.T) {
		const jsonStream = `
		{"Name": "Ed", "Text": "Knock knock."}
		{"Name": "Sam", "Text": "Who's there?"}
		{"Name": "Ed", "Text": "Go fmt."}
		{"Name": "Sam", "Text": "Go fmt who?"}
		{"Name": "Ed", "Text": "Go fmt yourself!"}
		`
		jsonStreamResult := []string{`{"Name": "Ed", "Text": "Knock knock."}`,
			`{"Name": "Sam", "Text": "Who's there?"}`, `{"Name": "Ed", "Text": "Go fmt."}`,
			`{"Name": "Sam", "Text": "Go fmt who?"}`,
			`{"Name": "Ed", "Text": "Go fmt yourself!"}`}

		db, err := genji.Open(":memory:")
		defer db.Close()
		assert.NoError(t, err)

		err = db.Exec(`CREATE TABLE foo`)
		assert.NoError(t, err)

		err = InsertJSON(db, "foo", strings.NewReader(jsonStream))
		assert.NoError(t, err)

		res, err := db.Query("SELECT * FROM foo")
		defer res.Close()
		assert.NoError(t, err)

		i := 0
		_ = res.Iterate(func(d types.Document) error {
			data, err := document.MarshalJSON(d)
			assert.NoError(t, err)
			require.JSONEq(t, jsonStreamResult[i], string(data))
			i++
			return nil
		})

		wantCount := 0
		err = res.Iterate(func(d types.Document) error {
			wantCount++
			return nil
		})
		assert.NoError(t, err)
		require.Equal(t, wantCount, i)
	})
}
