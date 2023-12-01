package expr_test

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/genjidb/genji/internal/database"
	"github.com/genjidb/genji/internal/environment"
	"github.com/genjidb/genji/internal/expr"
	"github.com/genjidb/genji/internal/object"
	"github.com/genjidb/genji/internal/sql/parser"
	"github.com/genjidb/genji/internal/testutil"
	"github.com/genjidb/genji/internal/testutil/assert"
	"github.com/genjidb/genji/internal/types"
	"github.com/stretchr/testify/require"
)

func TestPathExpr(t *testing.T) {
	tests := []struct {
		expr  string
		res   types.Value
		fails bool
	}{
		{"a", types.NewIntegerValue(1), false},
		{"b", func() types.Value {
			fb := object.NewFieldBuffer()
			err := json.Unmarshal([]byte(`{"foo bar": [1, 2]}`), fb)
			assert.NoError(t, err)
			return types.NewObjectValue(fb)
		}(),
			false},
		{"b.`foo bar`[0]", types.NewIntegerValue(1), false},
		{"_v.b.`foo bar`[0]", types.NewNullValue(), false},
		{"b.`foo bar`[1]", types.NewIntegerValue(2), false},
		{"b.`foo bar`[2]", nullLiteral, false},
		{"b[0]", nullLiteral, false},
		{"c[0]", types.NewIntegerValue(1), false},
		{"c[1].foo", types.NewTextValue("bar"), false},
		{"c.foo", nullLiteral, false},
		{"d", nullLiteral, false},
	}

	r := database.NewBasicRow(object.NewFromJSON([]byte(`{
		"a": 1,
		"b": {"foo bar": [1, 2]},
		"c": [1, {"foo": "bar"}, [1, 2]]
	}`)))

	for _, test := range tests {
		t.Run(test.expr, func(t *testing.T) {
			testutil.TestExpr(t, test.expr, environment.New(r), test.res, test.fails)
		})
	}

	t.Run("empty env", func(t *testing.T) {
		testutil.TestExpr(t, "a", &environment.Environment{}, nullLiteral, true)
	})
}

func TestPathIsEqual(t *testing.T) {
	tests := []struct {
		a, b    string
		isEqual bool
	}{
		{`a`, `a`, true},
		{`a[0].b`, `a[0].b`, true},
		{`a[0].b`, `a[1].b`, false},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%s = %s", test.a, test.b), func(t *testing.T) {
			pa, err := parser.ParsePath(test.a)
			assert.NoError(t, err)
			ea := expr.Path(pa)

			pb, err := parser.ParsePath(test.b)
			assert.NoError(t, err)
			eb := expr.Path(pb)

			require.Equal(t, test.isEqual, ea.IsEqual(eb))
		})
	}
}

func TestEnvPathExpr(t *testing.T) {
	tests := []struct {
		expr  string
		res   types.Value
		fails bool
	}{
		{"a", types.NewIntegerValue(1), false},
		{"b", func() types.Value {
			fb := object.NewFieldBuffer()
			err := json.Unmarshal([]byte(`{"foo bar": [1, 2]}`), fb)
			assert.NoError(t, err)
			return types.NewObjectValue(fb)
		}(),
			false},
		{"b.`foo bar`[0]", types.NewIntegerValue(1), false},
		{"_v.b.`foo bar`[0]", types.NewNullValue(), false},
		{"b.`foo bar`[1]", types.NewIntegerValue(2), false},
		{"b.`foo bar`[2]", nullLiteral, false},
		{"b[0]", nullLiteral, false},
		{"c[0]", types.NewIntegerValue(1), false},
		{"c[1].foo", types.NewTextValue("bar"), false},
		{"c.foo", nullLiteral, false},
		{"d", nullLiteral, false},
	}

	r := database.NewBasicRow(object.NewFromJSON([]byte(`{
		"a": 1,
		"b": {"foo bar": [1, 2]},
		"c": [1, {"foo": "bar"}, [1, 2]]
	}`)))

	for _, test := range tests {
		t.Run(test.expr, func(t *testing.T) {
			testutil.TestExpr(t, test.expr, environment.New(r), test.res, test.fails)
		})
	}

	t.Run("empty env", func(t *testing.T) {
		testutil.TestExpr(t, "a", &environment.Environment{}, nullLiteral, true)
	})
}
