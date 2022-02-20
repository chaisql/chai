package document_test

import (
	"testing"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/internal/testutil"
	"github.com/genjidb/genji/types"
	"github.com/stretchr/testify/require"
)

func TestDiff(t *testing.T) {
	tests := []struct {
		name   string
		d1, d2 string
		want   []document.Op
	}{
		{
			name: "empty",
			d1:   `{}`,
			d2:   `{}`,
			want: nil,
		},
		{
			name: "add field",
			d1:   `{}`,
			d2:   `{"a": 1}`,
			want: []document.Op{
				{"set", document.NewPath("a"), types.NewIntegerValue(1)},
			},
		},
		{
			name: "remove field",
			d1:   `{"a": 1}`,
			d2:   `{}`,
			want: []document.Op{
				{"delete", document.NewPath("a"), types.NewIntegerValue(1)},
			},
		},
		{
			name: "same",
			d1:   `{"a": 1}`,
			d2:   `{"a": 1}`,
			want: nil,
		},
		{
			name: "replace field",
			d1:   `{"a": 1}`,
			d2:   `{"a": 2}`,
			want: []document.Op{
				{"set", document.NewPath("a"), types.NewIntegerValue(2)},
			},
		},
		{
			name: "replace field: different type",
			d1:   `{"a": 1}`,
			d2:   `{"a": "hello"}`,
			want: []document.Op{
				{"set", document.NewPath("a"), types.NewTextValue("hello")},
			},
		},
		{
			name: "nested document: replace field",
			d1:   `{"a": {"b": 1}}`,
			d2:   `{"a": {"b": 2}}`,
			want: []document.Op{
				{"set", document.NewPath("a", "b"), types.NewIntegerValue(2)},
			},
		},
		{
			name: "nested document: add field",
			d1:   `{"a": {"b": 1}}`,
			d2:   `{"a": {"b": 1, "c": 2}}`,
			want: []document.Op{
				{"set", document.NewPath("a", "c"), types.NewIntegerValue(2)},
			},
		},
		{
			name: "nested document: remove field",
			d1:   `{"a": {"b": 1, "c": 2}}`,
			d2:   `{"a": {"b": 1}}`,
			want: []document.Op{
				{"delete", document.NewPath("a", "c"), types.NewIntegerValue(2)},
			},
		},
		{
			name: "nested array: replace index",
			d1:   `{"a": [1, 2, 3]}`,
			d2:   `{"a": [1, 2, 4]}`,
			want: []document.Op{
				{"set", document.NewPath("a", "2"), types.NewIntegerValue(4)},
			},
		},
		{
			name: "nested array: replace index with different type",
			d1:   `{"a": [1, 2, 3]}`,
			d2:   `{"a": [1, 2, 4.5]}`,
			want: []document.Op{
				{"set", document.NewPath("a", "2"), types.NewDoubleValue(4.5)},
			},
		},
		{
			name: "nested array: add index",
			d1:   `{"a": [1, 2, 3]}`,
			d2:   `{"a": [1, 2, 3, 4]}`,
			want: []document.Op{
				{"set", document.NewPath("a", "3"), types.NewIntegerValue(4)},
			},
		},
		{
			name: "nested array: remove index",
			d1:   `{"a": [1, 2, 3, 4]}`,
			d2:   `{"a": [1, 2, 3]}`,
			want: []document.Op{
				{"delete", document.NewPath("a", "3"), types.NewIntegerValue(4)},
			},
		},
		{
			name: "nested array: add in the middle",
			d1:   `{"a": [1, 2, 3]}`,
			d2:   `{"a": [1, 2, 2.5, 3]}`,
			want: []document.Op{
				{"set", document.NewPath("a", "2"), types.NewDoubleValue(2.5)},
				{"set", document.NewPath("a", "3"), types.NewIntegerValue(3)},
			},
		},
		{
			name: "nested array: with nested array",
			d1:   `{"a": [1, 2, []]}`,
			d2:   `{"a": [1, 2, [1], 3]}`,
			want: []document.Op{
				{"set", document.NewPath("a", "2", "0"), types.NewIntegerValue(1)},
				{"set", document.NewPath("a", "3"), types.NewIntegerValue(3)},
			},
		},
		{
			name: "nested array: with nested document",
			d1:   `{"a": [1, 2, {"b": [1]}]}`,
			d2:   `{"a": [1, 2, {"b": [2]}, 3]}`,
			want: []document.Op{
				{"set", document.NewPath("a", "2", "b", "0"), types.NewIntegerValue(2)},
				{"set", document.NewPath("a", "3"), types.NewIntegerValue(3)},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			d1 := testutil.MakeDocument(t, test.d1)
			d2 := testutil.MakeDocument(t, test.d2)

			got, err := document.Diff(d1, d2)
			require.NoError(t, err)
			require.Equal(t, test.want, got)
		})
	}
}
