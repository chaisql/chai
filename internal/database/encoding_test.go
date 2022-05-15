package database_test

import (
	"bytes"
	"testing"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/internal/database"
	"github.com/genjidb/genji/internal/expr"
	"github.com/genjidb/genji/internal/testutil"
	"github.com/genjidb/genji/types"
	"github.com/stretchr/testify/require"
)

func TestEncoding(t *testing.T) {
	var ti database.TableInfo

	err := ti.AddFieldConstraint(&database.FieldConstraint{
		Position: 0,
		Field:    "a",
		Type:     types.IntegerValue,
	})
	require.NoError(t, err)

	err = ti.AddFieldConstraint(&database.FieldConstraint{
		Position: 1,
		Field:    "b",
		Type:     types.TextValue,
	})
	require.NoError(t, err)

	err = ti.AddFieldConstraint(&database.FieldConstraint{
		Position:  2,
		Field:     "c",
		Type:      types.DoubleValue,
		IsNotNull: true,
	})
	require.NoError(t, err)

	err = ti.AddFieldConstraint(&database.FieldConstraint{
		Position:     3,
		Field:        "d",
		Type:         types.DoubleValue,
		DefaultValue: expr.Constraint(testutil.ParseExpr(t, `10`)),
	})
	require.NoError(t, err)

	err = ti.AddFieldConstraint(&database.FieldConstraint{
		Position: 4,
		Field:    "e",
		Type:     types.DoubleValue,
	})
	require.NoError(t, err)

	ti.FieldConstraints.AllowExtraFields = true

	codec := database.NewCodec(nil, &ti)

	doc := document.NewFromMap(map[string]any{
		"a":     int64(1),
		"b":     "hello",
		"c":     float64(3.14),
		"e":     int64(100),
		"f":     int64(1000),
		"g":     float64(2000),
		"array": []int{1, 2, 3},
		"doc":   document.NewFromMap(map[string]int64{"a": 10}),
	})

	var buf bytes.Buffer
	err = codec.Encode(&buf, doc)
	require.NoError(t, err)

	d, err := codec.Decode(buf.Bytes())
	require.NoError(t, err)

	want := document.NewFromMap(map[string]any{
		"a":     int64(1),
		"b":     "hello",
		"c":     float64(3.14),
		"d":     float64(10),
		"e":     float64(100),
		"f":     float64(1000),
		"g":     float64(2000),
		"array": []float64{1, 2, 3},
		"doc":   document.NewFromMap(map[string]float64{"a": 10}),
	})

	testutil.RequireDocEqual(t, want, d)
}
