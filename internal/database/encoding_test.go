package database_test

import (
	"testing"

	"github.com/genjidb/genji/internal/database"
	"github.com/genjidb/genji/internal/expr"
	"github.com/genjidb/genji/internal/testutil"
	"github.com/genjidb/genji/object"
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

	doc := object.NewFromMap(map[string]any{
		"a":     int64(1),
		"b":     "hello",
		"c":     float64(3.14),
		"e":     int64(100),
		"f":     int64(1000),
		"g":     float64(2000),
		"array": []int{1, 2, 3},
		"doc":   object.NewFromMap(map[string]int64{"a": 10}),
	})

	var buf []byte
	buf, err = ti.EncodeObject(nil, buf, doc)
	require.NoError(t, err)

	d := database.NewEncodedObject(&ti.FieldConstraints, buf)
	require.NoError(t, err)

	want := object.NewFromMap(map[string]any{
		"a":     int64(1),
		"b":     "hello",
		"c":     float64(3.14),
		"d":     float64(10),
		"e":     float64(100),
		"f":     float64(1000),
		"g":     float64(2000),
		"array": []float64{1, 2, 3},
		"doc":   object.NewFromMap(map[string]float64{"a": 10}),
	})

	testutil.RequireObjEqual(t, want, d)

	t.Run("with nested objects", func(t *testing.T) {
		var ti database.TableInfo

		// a OBJECT(...)
		err := ti.AddFieldConstraint(&database.FieldConstraint{
			Position: 0,
			Field:    "a",
			Type:     types.ObjectValue,
			AnonymousType: &database.AnonymousType{
				FieldConstraints: database.FieldConstraints{
					AllowExtraFields: true,
				},
			},
		})
		require.NoError(t, err)

		// b OBJECT(d TEST)
		var subfcs database.FieldConstraints
		err = subfcs.Add(&database.FieldConstraint{
			Position: 0,
			Field:    "d",
			Type:     types.TextValue,
		})
		subfcs.AllowExtraFields = true
		require.NoError(t, err)

		err = ti.AddFieldConstraint(&database.FieldConstraint{
			Position: 1,
			Field:    "b",
			Type:     types.ObjectValue,
			AnonymousType: &database.AnonymousType{
				FieldConstraints: subfcs,
			},
		})
		require.NoError(t, err)

		// c INT
		err = ti.AddFieldConstraint(&database.FieldConstraint{
			Position: 2,
			Field:    "c",
			Type:     types.IntegerValue,
		})
		require.NoError(t, err)

		doc := object.NewFromMap(map[string]any{
			"a": object.WithSortedFields(object.NewFromMap(map[string]any{"w": "hello", "x": int64(1)})),
			"b": object.WithSortedFields(object.NewFromMap(map[string]any{"d": "bye", "e": int64(2)})),
			"c": int64(100),
		})

		got, err := ti.EncodeObject(nil, nil, doc)
		require.NoError(t, err)

		d := database.NewEncodedObject(&ti.FieldConstraints, got)
		require.NoError(t, err)

		want := object.NewFromMap(map[string]any{
			"a": object.WithSortedFields(object.NewFromMap(map[string]any{"w": "hello", "x": float64(1)})),
			"b": object.WithSortedFields(object.NewFromMap(map[string]any{"d": "bye", "e": float64(2)})),
			"c": int64(100),
		})

		testutil.RequireObjEqual(t, want, d)
	})
}
