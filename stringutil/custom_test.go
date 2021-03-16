package stringutil

import (
	"bytes"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSprintf(t *testing.T) {
	tests := []struct {
		name     string
		msg      string
		args     []interface{}
		expected string
	}{
		{"no args", "foo", nil, "foo"},
		{"%s", "foo %s %s %d %v %c %v", []interface{}{"a", bytes.NewBufferString("b"), 3, 4, '6', []byte{1, 2}}, "foo a b 3 4 6 [1, 2]"},
		{"%q", "foo %q", []interface{}{"a"}, "foo \"a\""},
		{"%w", "foo %w %w", []interface{}{"a", errors.New("b")}, "foo a b"},
		{"%z", "foo %z", []interface{}{"a"}, "foo %z"},
		{"%s %q", "foo %s %q", []interface{}{"a", "b"}, "foo a \"b\""},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := sprintf(test.msg, test.args...)
			require.Equal(t, got, test.expected)
		})
	}
}
