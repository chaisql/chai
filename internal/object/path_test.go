package object_test

import (
	"encoding/json"
	"testing"

	"github.com/genjidb/genji/internal/object"
	"github.com/genjidb/genji/internal/sql/parser"
	"github.com/genjidb/genji/internal/testutil/assert"
	"github.com/stretchr/testify/require"
)

func TestPath(t *testing.T) {
	tests := []struct {
		name   string
		data   string
		path   string
		result string
		fails  bool
	}{
		{"root", `{"a": {"b": [1, 2, 3]}}`, `a`, `{"b": [1, 2, 3]}`, false},
		{"nested doc", `{"a": {"b": [1, 2, 3]}}`, `a.b`, `[1, 2, 3]`, false},
		{"nested array", `{"a": {"b": [1, 2, 3]}}`, `a.b[1]`, `2`, false},
		{"index out of range", `{"a": {"b": [1, 2, 3]}}`, `a.b[1000]`, ``, true},
		{"number field", `{"a": {"0": [1, 2, 3]}}`, "a.`0`", `[1, 2, 3]`, false},
		{"letter index", `{"a": {"b": [1, 2, 3]}}`, `a.b.c`, ``, true},
		{"unknown path", `{"a": {"b": [1, 2, 3]}}`, `a.e.f`, ``, true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var buf object.FieldBuffer

			err := json.Unmarshal([]byte(test.data), &buf)
			assert.NoError(t, err)
			p, err := parser.ParsePath(test.path)
			assert.NoError(t, err)
			v, err := p.GetValueFromObject(&buf)
			if test.fails {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				res, err := json.Marshal(v)
				assert.NoError(t, err)
				require.JSONEq(t, test.result, string(res))
			}
		})
	}
}
