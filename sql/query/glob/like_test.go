package glob

import (
	"testing"
)

func TestMatchLike(t *testing.T) {
	tests := []struct {
		s, pattern string
		want       bool
	}{
		// Empty
		{"", "", true},
		{"abc", "", false},

		// One
		{"x", "_", true},
		{"xx", "_", false},
		{"", "_", false},

		// Any
		{"abc", "%", true},
		{"", "%", true},

		// Escape
		{"%", "\\%", true},
		{"_", "\\_", true},
		{"x", "\\%", false},
		{"x", "\\_", false},
		{"x", "\\x", true},

		// Escaping escape
		{"\\", "\\\\", true},
		{"\\", "\\\\%", true},
		{"\\", "\\\\_", false},
		{"\\x", "\\\\x", true},

		// Exact
		{"abc", "abc", true},
		{"aBc", "AbC", false},
		{"abc", "def", false},

		// Prefix
		{"abcdef", "abc%", true},
		{"abcdef", "def%", false},

		// Suffix
		{"defabc", "%abc", true},
		{"defabc", "%def", false},

		// Contains
		{"defabcdef", "%abc%", true},
		{"abcd", "%def%", false},
		{"abc", "b", false},

		// Complex
		{"ABCD", "%B%C%", true},
		{"ABCD", "_%B%C%_", true},
		{"ABxCxxD", "a%b%c%d", true},
		{"ABxCxxD", "%__B", false},
	}

	for _, test := range tests {
		if got := MatchLike(test.pattern, test.s); got != test.want {
			t.Errorf(
				"MatchLike(%#v, %#v): expected %#v, got %#v",
				test.pattern, test.s, test.want, got,
			)
		}
	}
}
