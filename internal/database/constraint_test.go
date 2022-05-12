package database_test

import (
	"fmt"
	"testing"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/internal/database"
	"github.com/genjidb/genji/internal/expr"
	"github.com/genjidb/genji/internal/testutil"
	"github.com/genjidb/genji/internal/testutil/assert"
	"github.com/genjidb/genji/types"
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
			[]*database.FieldConstraint{{Field: "a", Type: types.IntegerValue}},
			database.FieldConstraint{Field: "a", Type: types.IntegerValue},
			nil,
			true,
		},
		{
			"Different path",
			[]*database.FieldConstraint{{Field: "a", Type: types.IntegerValue}},
			database.FieldConstraint{Field: "b", Type: types.IntegerValue},
			[]*database.FieldConstraint{
				{Field: "a", Type: types.IntegerValue},
				{Field: "b", Type: types.IntegerValue},
			},
			false,
		},
		{
			"Default value conversion, typed constraint",
			[]*database.FieldConstraint{{Field: "a", Type: types.IntegerValue}},
			database.FieldConstraint{Field: "b", Type: types.IntegerValue, DefaultValue: expr.Constraint(testutil.DoubleValue(5))},
			[]*database.FieldConstraint{
				{Field: "a", Type: types.IntegerValue},
				{Field: "b", Type: types.IntegerValue, DefaultValue: expr.Constraint(testutil.DoubleValue(5))},
			},
			false,
		},
		{
			"Default value conversion, typed constraint, NEXT VALUE FOR",
			[]*database.FieldConstraint{{Field: "a", Type: types.IntegerValue}},
			database.FieldConstraint{Field: "b", Type: types.IntegerValue, DefaultValue: expr.Constraint(expr.NextValueFor{SeqName: "seq"})},
			[]*database.FieldConstraint{
				{Field: "a", Type: types.IntegerValue},
				{Field: "b", Type: types.IntegerValue, DefaultValue: expr.Constraint(expr.NextValueFor{SeqName: "seq"})},
			},
			false,
		},
		{
			"Default value conversion, typed constraint, NEXT VALUE FOR with blob",
			[]*database.FieldConstraint{{Field: "a", Type: types.IntegerValue}},
			database.FieldConstraint{Field: "b", Type: types.BlobValue, DefaultValue: expr.Constraint(expr.NextValueFor{SeqName: "seq"})},
			nil,
			true,
		},
		{
			"Default value conversion, typed constraint, incompatible value",
			[]*database.FieldConstraint{{Field: "a", Type: types.IntegerValue}},
			database.FieldConstraint{Field: "b", Type: types.DoubleValue, DefaultValue: expr.Constraint(testutil.BoolValue(true))},
			nil,
			true,
		},
		{
			"Default value conversion, untyped constraint",
			[]*database.FieldConstraint{{Field: "a", Type: types.IntegerValue}},
			database.FieldConstraint{Field: "b", DefaultValue: expr.Constraint(testutil.IntegerValue(5))},
			[]*database.FieldConstraint{
				{Field: "a", Type: types.IntegerValue},
				{Field: "b", DefaultValue: expr.Constraint(testutil.IntegerValue(5))},
			},
			false,
		},
		{
			"Default value on nested document field",
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
		path        document.Path
		in, want    types.Value
		fails       bool
	}{
		{
			nil,
			document.NewPath("a"),
			types.NewIntegerValue(10),
			types.NewDoubleValue(10),
			false,
		},
		{
			[]*database.FieldConstraint{{Field: "a", Type: types.IntegerValue}},
			document.NewPath("a"),
			types.NewIntegerValue(10),
			types.NewIntegerValue(10),
			false,
		},
		{
			[]*database.FieldConstraint{{Field: "a", Type: types.IntegerValue}},
			document.NewPath("a"),
			types.NewDoubleValue(10.5),
			types.NewIntegerValue(10),
			false,
		},
		{
			[]*database.FieldConstraint{{Field: "a", Type: types.ArrayValue}},
			document.NewPath("a"),
			types.NewArrayValue(testutil.MakeArray(t, `[10.5, 10.5]`)),
			types.NewArrayValue(testutil.MakeArray(t, `[10.5, 10.5]`)),
			false,
		},
		{
			[]*database.FieldConstraint{{
				Field: "a",
				Type:  types.DocumentValue,
				AnonymousType: &database.AnonymousType{
					FieldConstraints: database.MustNewFieldConstraints(&database.FieldConstraint{
						Field: "b",
						Type:  types.IntegerValue,
					})}}},
			document.NewPath("a"),
			types.NewDocumentValue(testutil.MakeDocument(t, `{"b": 10.5, "c": 10.5}`)),
			types.NewDocumentValue(testutil.MakeDocument(t, `{"b": 10, "c": 10.5}`)),
			false,
		},
		{
			[]*database.FieldConstraint{{Field: "a", Type: types.IntegerValue}},
			document.NewPath("a"),
			types.NewTextValue("foo"),
			types.NewTextValue("foo"),
			true,
		},
		{
			[]*database.FieldConstraint{{Field: "a", DefaultValue: expr.Constraint(testutil.IntegerValue(10))}},
			document.NewPath("a"),
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
