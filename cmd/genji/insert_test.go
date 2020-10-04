package main

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/genjidb/genji"
	"github.com/genjidb/genji/document"
	"github.com/stretchr/testify/require"
)

func TestExecuteInsertCommand(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name  string
		data  string
		want  string
		fails bool
	}{
		{"Simple Json", `{"a": 1}`, `{"a": 1}`, false},
		{"JSON object", `{"a": {"b": [1, 2, 3]}}`, `{"a": {"b": [1, 2, 3]}}`, false},
		{"nested document", `{"a": {"b": [1, 2, 3]}}`, `{"a": {"b": [1, 2, 3]}}`, false},
		{"nested array multiple indexes", `{"a": {"b": [1, 2, [1, 2, {"c": "foo"}]]}}`, `{"a": {"b": [1, 2, [1, 2, {"c": "foo"}]]}}`, false},
		{"document in array", `{"a": [{"b":"foo"}, 2, 3]}`, `{"a": [{"b":"foo"}, 2, 3]}`, false},
		{"Non closed json array", `[{"foo":"bar"}`, ``, true},
		{"Non closed json stream", `{"foo":"bar"`, ``, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, err := genji.Open(":memory:")
			require.NoError(t, err)
			defer db.Close()

			err = db.Exec(ctx, `CREATE TABLE foo`)
			require.NoError(t, err)
			err = executeInsertCommand(context.Background(), db, "foo", strings.NewReader(tt.data))
			if tt.fails {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			res, err := db.Query(ctx, "SELECT * FROM foo")
			defer res.Close()
			require.NoError(t, err)

			var buf bytes.Buffer
			err = document.IteratorToJSON(&buf, res)
			require.NoError(t, err)
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
		require.NoError(t, err)
		defer db.Close()

		err = db.Exec(ctx, `CREATE TABLE foo`)
		require.NoError(t, err)
		err = executeInsertCommand(context.Background(), db, "foo", strings.NewReader(jsonArray))
		require.NoError(t, err)
		res, err := db.Query(ctx, "SELECT * FROM foo")
		defer res.Close()
		require.NoError(t, err)

		i := 0
		_ = res.Iterate(func(d document.Document) error {
			data, err := document.MarshalJSON(d)
			require.NoError(t, err)
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
		require.NoError(t, err)

		err = db.Exec(ctx, `CREATE TABLE foo`)
		require.NoError(t, err)

		err = executeInsertCommand(context.Background(), db, "foo", strings.NewReader(jsonStream))
		require.NoError(t, err)

		res, err := db.Query(ctx, "SELECT * FROM foo")
		defer res.Close()
		require.NoError(t, err)

		i := 0
		_ = res.Iterate(func(d document.Document) error {
			data, err := document.MarshalJSON(d)
			require.NoError(t, err)
			require.JSONEq(t, jsonStreamResult[i], string(data))
			i++
			return nil
		})

		wantCount, err := res.Count()
		require.NoError(t, err)
		require.Equal(t, wantCount, i)
	})

}
