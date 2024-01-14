package database_test

import (
	"fmt"
	"testing"

	"github.com/chaisql/chai/internal/database"
	"github.com/chaisql/chai/internal/expr"
	"github.com/chaisql/chai/internal/object"
	"github.com/chaisql/chai/internal/testutil"
	"github.com/chaisql/chai/internal/testutil/assert"
	"github.com/chaisql/chai/internal/types"
	"github.com/stretchr/testify/require"
)

func TestFieldConstraintsAdd(t *testing.T) {
	tests := []struct {
		name  string
		got   []*database.FieldConstraint
		add   database.FieldConstraint
		want  []*database.FieldConstraint
		fails bool
	}{
		{
			"Same path",
			[]*database.FieldConstraint{{Field: "a", Type: types.TypeInteger}},
			database.FieldConstraint{Field: "a", Type: types.TypeInteger},
			nil,
			true,
		},
		{
			"Different path",
			[]*database.FieldConstraint{{Field: "a", Type: types.TypeInteger}},
			database.FieldConstraint{Field: "b", Type: types.TypeInteger},
			[]*database.FieldConstraint{
				{Position: 0, Field: "a", Type: types.TypeInteger},
				{Position: 1, Field: "b", Type: types.TypeInteger},
			},
			false,
		},
		{
			"Default value conversion, typed constraint",
			[]*database.FieldConstraint{{Field: "a", Type: types.TypeInteger}},
			database.FieldConstraint{Field: "b", Type: types.TypeInteger, DefaultValue: expr.Constraint(testutil.DoubleValue(5))},
			[]*database.FieldConstraint{
				{Position: 0, Field: "a", Type: types.TypeInteger},
				{Position: 1, Field: "b", Type: types.TypeInteger, DefaultValue: expr.Constraint(testutil.DoubleValue(5))},
			},
			false,
		},
		{
			"Default value conversion, typed constraint, NEXT VALUE FOR",
			[]*database.FieldConstraint{{Field: "a", Type: types.TypeInteger}},
			database.FieldConstraint{Field: "b", Type: types.TypeInteger, DefaultValue: expr.Constraint(expr.NextValueFor{SeqName: "seq"})},
			[]*database.FieldConstraint{
				{Position: 0, Field: "a", Type: types.TypeInteger},
				{Position: 1, Field: "b", Type: types.TypeInteger, DefaultValue: expr.Constraint(expr.NextValueFor{SeqName: "seq"})},
			},
			false,
		},
		{
			"Default value conversion, typed constraint, NEXT VALUE FOR with blob",
			[]*database.FieldConstraint{{Field: "a", Type: types.TypeInteger}},
			database.FieldConstraint{Field: "b", Type: types.TypeBlob, DefaultValue: expr.Constraint(expr.NextValueFor{SeqName: "seq"})},
			nil,
			true,
		},
		{
			"Default value conversion, typed constraint, incompatible value",
			[]*database.FieldConstraint{{Field: "a", Type: types.TypeInteger}},
			database.FieldConstraint{Field: "b", Type: types.TypeDouble, DefaultValue: expr.Constraint(testutil.BoolValue(true))},
			nil,
			true,
		},
		{
			"Default value conversion, untyped constraint",
			[]*database.FieldConstraint{{Field: "a", Type: types.TypeInteger}},
			database.FieldConstraint{Field: "b", DefaultValue: expr.Constraint(testutil.IntegerValue(5))},
			[]*database.FieldConstraint{
				{Position: 0, Field: "a", Type: types.TypeInteger},
				{Position: 1, Field: "b", DefaultValue: expr.Constraint(testutil.IntegerValue(5))},
			},
			false,
		},
		{
			"Default value on nested object column",
			nil,
			database.FieldConstraint{Field: "a.b", DefaultValue: expr.Constraint(testutil.IntegerValue(5))},
			[]*database.FieldConstraint{
				{Field: "a.b", DefaultValue: expr.Constraint(testutil.IntegerValue(5))},
			},
			false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fcs := database.MustNewFieldConstraints(test.got...)
			err := fcs.Add(&test.add)
			if test.fails {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				require.Equal(t, test.want, fcs.Ordered)
			}
		})
	}
}

func TestFieldConstraintsConvert(t *testing.T) {
	tests := []struct {
		constraints []*database.FieldConstraint
		path        object.Path
		in, want    types.Value
		fails       bool
	}{
		{
			nil,
			object.NewPath("a"),
			types.NewIntegerValue(10),
			types.NewDoubleValue(10),
			false,
		},
		{
			[]*database.FieldConstraint{{Field: "a", Type: types.TypeInteger}},
			object.NewPath("a"),
			types.NewIntegerValue(10),
			types.NewIntegerValue(10),
			false,
		},
		{
			[]*database.FieldConstraint{{Field: "a", Type: types.TypeInteger}},
			object.NewPath("a"),
			types.NewDoubleValue(10.5),
			types.NewIntegerValue(10),
			false,
		},
		{
			[]*database.FieldConstraint{{Field: "a", Type: types.TypeArray}},
			object.NewPath("a"),
			types.NewArrayValue(testutil.MakeArray(t, `[10.5, 10.5]`)),
			types.NewArrayValue(testutil.MakeArray(t, `[10.5, 10.5]`)),
			false,
		},
		{
			[]*database.FieldConstraint{{
				Field: "a",
				Type:  types.TypeObject,
				AnonymousType: &database.AnonymousType{
					FieldConstraints: database.MustNewFieldConstraints(&database.FieldConstraint{
						Field: "b",
						Type:  types.TypeInteger,
					})}}},
			object.NewPath("a"),
			types.NewObjectValue(testutil.MakeObject(t, `{"b": 10.5, "c": 10.5}`)),
			types.NewObjectValue(testutil.MakeObject(t, `{"b": 10, "c": 10.5}`)),
			false,
		},
		{
			[]*database.FieldConstraint{{Field: "a", Type: types.TypeInteger}},
			object.NewPath("a"),
			types.NewTextValue("foo"),
			types.NewTextValue("foo"),
			true,
		},
		{
			[]*database.FieldConstraint{{Field: "a", DefaultValue: expr.Constraint(testutil.IntegerValue(10))}},
			object.NewPath("a"),
			types.NewTextValue("foo"),
			types.NewTextValue("foo"),
			false,
		},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%s / %v to %v", test.path, test.in, test.want), func(t *testing.T) {
			got, err := database.MustNewFieldConstraints(test.constraints...).ConvertValueAtPath(test.path, test.in, database.CastConversion)
			if test.fails {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				require.Equal(t, test.want, got)
			}
		})
	}
}
