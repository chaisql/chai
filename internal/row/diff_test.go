package row_test

import (
	"testing"

	"github.com/chaisql/chai/internal/row"
	"github.com/chaisql/chai/internal/testutil"
	"github.com/chaisql/chai/internal/types"
	"github.com/stretchr/testify/require"
)

func TestDiff(t *testing.T) {
	tests := []struct {
		name   string
		d1, d2 string
		want   []row.Op
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
			want: []row.Op{
				{"set", "a", types.NewIntegerValue(1)},
			},
		},
		{
			name: "remove field",
			d1:   `{"a": 1}`,
			d2:   `{}`,
			want: []row.Op{
				{"delete", "a", types.NewIntegerValue(1)},
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
			want: []row.Op{
				{"set", "a", types.NewIntegerValue(2)},
			},
		},
		{
			name: "replace field: different type",
			d1:   `{"a": 1}`,
			d2:   `{"a": "hello"}`,
			want: []row.Op{
				{"set", "a", types.NewTextValue("hello")},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			d1 := testutil.MakeRow(t, test.d1)
			d2 := testutil.MakeRow(t, test.d2)

			got, err := row.Diff(d1, d2)
			require.NoError(t, err)
			require.Equal(t, test.want, got)
		})
	}
}
