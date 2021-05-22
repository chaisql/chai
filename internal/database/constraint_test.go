package database_test

import (
	"fmt"
	"testing"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/internal/database"
	"github.com/genjidb/genji/internal/testutil"
	"github.com/stretchr/testify/require"
)

func TestFieldConstraintsInfer(t *testing.T) {
	tests := []struct {
		name      string
		got, want database.FieldConstraints
		fails     bool
	}{
		{
			"No change",
			[]*database.FieldConstraint{{Path: document.NewPath("a"), Type: document.IntegerValue}},
			[]*database.FieldConstraint{{Path: document.NewPath("a"), Type: document.IntegerValue}},
			false,
		},
		{
			"Array",
			[]*database.FieldConstraint{{Path: document.NewPath("a", "0"), Type: document.IntegerValue}},
			[]*database.FieldConstraint{
				{Path: document.NewPath("a"), Type: document.ArrayValue, IsInferred: true, InferredBy: []document.Path{document.NewPath("a", "0")}},
				{Path: document.NewPath("a", "0"), Type: document.IntegerValue},
			},
			false,
		},
		{
			"Document",
			[]*database.FieldConstraint{{Path: document.NewPath("a", "b"), Type: document.IntegerValue}},
			[]*database.FieldConstraint{
				{Path: document.NewPath("a"), Type: document.DocumentValue, IsInferred: true, InferredBy: []document.Path{document.NewPath("a", "b")}},
				{Path: document.NewPath("a", "b"), Type: document.IntegerValue},
			},
			false,
		},
		{
			"Primary key",
			[]*database.FieldConstraint{
				{Path: document.NewPath("a"), Type: document.ArrayValue, IsPrimaryKey: true},
				{Path: document.NewPath("a", "0"), Type: document.IntegerValue},
			},
			[]*database.FieldConstraint{
				{Path: document.NewPath("a"), Type: document.ArrayValue, IsPrimaryKey: true},
				{Path: document.NewPath("a", "0"), Type: document.IntegerValue},
			},
			false,
		},
		{
			"Complex path",
			[]*database.FieldConstraint{{Path: document.NewPath("a", "b", "3", "1", "c"), Type: document.IntegerValue}},
			[]*database.FieldConstraint{
				{Path: document.NewPath("a"), Type: document.DocumentValue, IsInferred: true, InferredBy: []document.Path{document.NewPath("a", "b", "3", "1", "c")}},
				{Path: document.NewPath("a", "b"), Type: document.ArrayValue, IsInferred: true, InferredBy: []document.Path{document.NewPath("a", "b", "3", "1", "c")}},
				{Path: document.NewPath("a", "b", "3"), Type: document.ArrayValue, IsInferred: true, InferredBy: []document.Path{document.NewPath("a", "b", "3", "1", "c")}},
				{Path: document.NewPath("a", "b", "3", "1"), Type: document.DocumentValue, IsInferred: true, InferredBy: []document.Path{document.NewPath("a", "b", "3", "1", "c")}},
				{Path: document.NewPath("a", "b", "3", "1", "c"), Type: document.IntegerValue},
			},
			false,
		},
		{
			"Overlaping constraints",
			[]*database.FieldConstraint{
				{Path: document.NewPath("a", "b"), Type: document.IntegerValue},
				{Path: document.NewPath("a", "c"), Type: document.IntegerValue},
			},
			[]*database.FieldConstraint{
				{Path: document.NewPath("a"), Type: document.DocumentValue, IsInferred: true, InferredBy: []document.Path{document.NewPath("a", "b"), document.NewPath("a", "c")}},
				{Path: document.NewPath("a", "b"), Type: document.IntegerValue},
				{Path: document.NewPath("a", "c"), Type: document.IntegerValue},
			},
			false,
		},
		{
			"Same path inferred and non inferred: inferred first",
			[]*database.FieldConstraint{
				{Path: document.NewPath("a", "b"), Type: document.IntegerValue},
				{Path: document.NewPath("a"), Type: document.DocumentValue},
			},
			[]*database.FieldConstraint{
				{Path: document.NewPath("a"), Type: document.DocumentValue},
				{Path: document.NewPath("a", "b"), Type: document.IntegerValue},
			},
			false,
		},
		{
			"Same path inferred and non inferred: inferred last",
			[]*database.FieldConstraint{
				{Path: document.NewPath("a"), Type: document.DocumentValue},
				{Path: document.NewPath("a", "b"), Type: document.IntegerValue},
			},
			[]*database.FieldConstraint{
				{Path: document.NewPath("a"), Type: document.DocumentValue},
				{Path: document.NewPath("a", "b"), Type: document.IntegerValue},
			},
			false,
		},
		{
			"Complex case",
			[]*database.FieldConstraint{
				{Path: document.NewPath("a", "b", "3", "1", "c"), Type: document.DocumentValue},
				{Path: document.NewPath("a", "b", "3", "1", "c", "d"), Type: document.IntegerValue},
				{Path: document.NewPath("a", "b", "2"), Type: document.IntegerValue},
			},
			[]*database.FieldConstraint{
				{Path: document.NewPath("a"), Type: document.DocumentValue, IsInferred: true,
					InferredBy: []document.Path{document.NewPath("a", "b", "3", "1", "c"), document.NewPath("a", "b", "3", "1", "c", "d"), document.NewPath("a", "b", "2")}},
				{Path: document.NewPath("a", "b"), Type: document.ArrayValue, IsInferred: true,
					InferredBy: []document.Path{document.NewPath("a", "b", "3", "1", "c"), document.NewPath("a", "b", "3", "1", "c", "d"), document.NewPath("a", "b", "2")}},
				{Path: document.NewPath("a", "b", "3"), Type: document.ArrayValue, IsInferred: true,
					InferredBy: []document.Path{document.NewPath("a", "b", "3", "1", "c"), document.NewPath("a", "b", "3", "1", "c", "d")}},
				{Path: document.NewPath("a", "b", "3", "1"), Type: document.DocumentValue, IsInferred: true,
					InferredBy: []document.Path{document.NewPath("a", "b", "3", "1", "c"), document.NewPath("a", "b", "3", "1", "c", "d")}},
				{Path: document.NewPath("a", "b", "3", "1", "c"), Type: document.DocumentValue},
				{Path: document.NewPath("a", "b", "3", "1", "c", "d"), Type: document.IntegerValue},
				{Path: document.NewPath("a", "b", "2"), Type: document.IntegerValue},
			},
			false,
		},
		{
			"Same path, different constraint",
			[]*database.FieldConstraint{
				{Path: document.NewPath("a", "b"), Type: document.IntegerValue},
				{Path: document.NewPath("a", "b"), Type: document.DoubleValue},
			},
			nil,
			true,
		},
		{
			"Inferred constraint first, conflict with non inferred",
			[]*database.FieldConstraint{
				{Path: document.NewPath("a", "b"), Type: document.IntegerValue},
				{Path: document.NewPath("a"), Type: document.IntegerValue},
			},
			nil,
			true,
		},
		{
			"Non inferred constraint first, conflict with inferred",
			[]*database.FieldConstraint{
				{Path: document.NewPath("a"), Type: document.IntegerValue},
				{Path: document.NewPath("a", "b"), Type: document.IntegerValue},
			},
			nil,
			true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := test.got.Infer()
			if test.fails {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, test.want, got)
			}
		})
	}
}

func TestFieldConstraintsAdd(t *testing.T) {
	tests := []struct {
		name  string
		got   database.FieldConstraints
		add   database.FieldConstraint
		want  database.FieldConstraints
		fails bool
	}{
		{
			"Same path",
			[]*database.FieldConstraint{{Path: document.NewPath("a"), Type: document.IntegerValue}},
			database.FieldConstraint{Path: document.NewPath("a"), Type: document.IntegerValue},
			nil,
			true,
		},
		{
			"Duplicate primary key",
			[]*database.FieldConstraint{{Path: document.NewPath("a"), IsPrimaryKey: true, Type: document.IntegerValue}},
			database.FieldConstraint{Path: document.NewPath("b"), IsPrimaryKey: true, Type: document.IntegerValue},
			nil,
			true,
		},
		{
			"Different path",
			[]*database.FieldConstraint{{Path: document.NewPath("a"), Type: document.IntegerValue}},
			database.FieldConstraint{Path: document.NewPath("b"), Type: document.IntegerValue},
			[]*database.FieldConstraint{
				{Path: document.NewPath("a"), Type: document.IntegerValue},
				{Path: document.NewPath("b"), Type: document.IntegerValue},
			},
			false,
		},
		{
			"Default value conversion, typed constraint",
			[]*database.FieldConstraint{{Path: document.NewPath("a"), Type: document.IntegerValue}},
			database.FieldConstraint{Path: document.NewPath("b"), Type: document.IntegerValue, DefaultValue: document.NewDoubleValue(5)},
			[]*database.FieldConstraint{
				{Path: document.NewPath("a"), Type: document.IntegerValue},
				{Path: document.NewPath("b"), Type: document.IntegerValue, DefaultValue: document.NewIntegerValue(5)},
			},
			false,
		},
		{
			"Default value conversion, untyped constraint",
			[]*database.FieldConstraint{{Path: document.NewPath("a"), Type: document.IntegerValue}},
			database.FieldConstraint{Path: document.NewPath("b"), DefaultValue: document.NewIntegerValue(5)},
			[]*database.FieldConstraint{
				{Path: document.NewPath("a"), Type: document.IntegerValue},
				{Path: document.NewPath("b"), DefaultValue: document.NewDoubleValue(5)},
			},
			false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.got.Add(&test.add)
			if test.fails {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, test.want, test.got)
			}
		})
	}
}

func TestFieldConstraintsConvert(t *testing.T) {
	tests := []struct {
		constraints database.FieldConstraints
		path        document.Path
		in, want    document.Value
		fails       bool
	}{
		{
			nil,
			document.NewPath("a"),
			document.NewIntegerValue(10),
			document.NewDoubleValue(10),
			false,
		},
		{
			database.FieldConstraints{{Path: document.NewPath("a"), Type: document.IntegerValue}},
			document.NewPath("a"),
			document.NewIntegerValue(10),
			document.NewIntegerValue(10),
			false,
		},
		{
			database.FieldConstraints{{Path: document.NewPath("a"), Type: document.IntegerValue}},
			document.NewPath("a"),
			document.NewDoubleValue(10.5),
			document.NewIntegerValue(10),
			false,
		},
		{
			database.FieldConstraints{{Path: document.NewPath("a", "0"), Type: document.IntegerValue}},
			document.NewPath("a"),
			document.NewArrayValue(testutil.MakeArray(t, `[10.5, 10.5]`)),
			document.NewArrayValue(testutil.MakeArray(t, `[10, 10.5]`)),
			false,
		},
		{
			database.FieldConstraints{{Path: document.NewPath("a", "b"), Type: document.IntegerValue}},
			document.NewPath("a"),
			document.NewDocumentValue(testutil.MakeDocument(t, `{"b": 10.5, "c": 10.5}`)),
			document.NewDocumentValue(testutil.MakeDocument(t, `{"b": 10, "c": 10.5}`)),
			false,
		},
		{
			database.FieldConstraints{{Path: document.NewPath("a"), Type: document.IntegerValue}},
			document.NewPath("a"),
			document.NewTextValue("foo"),
			document.NewTextValue("foo"),
			true,
		},
		{
			database.FieldConstraints{{Path: document.NewPath("a"), DefaultValue: document.NewIntegerValue(10)}},
			document.NewPath("a"),
			document.NewTextValue("foo"),
			document.NewTextValue("foo"),
			false,
		},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%s / %v to %v", test.path, test.in, test.want), func(t *testing.T) {
			got, err := test.constraints.ConvertValueAtPath(test.path, test.in, database.CastConversion)
			if test.fails {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, test.want, got)
			}
		})
	}
}
