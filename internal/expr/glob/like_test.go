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
		{"", "x", false},
		{"x", "", false},

		// One
		{"", "_", false},
		{"x", "_", true},
		{"x", "__", false},
		{"xx", "_", false},
		{"bLah", "bL_h", true},
		{"bLaaa", "bLa_", false},
		{"bLah", "bLa_", true},
		{"bLaH", "_Lah", true},
		{"bLaH", "_LaH", true},

		// All
		{"", "%", true},
		{"abc", "%", true},
		{"", "%", true},
		{"abc", "%%", true},

		// Any and one
		{"x", "%_", true},
		{"", "%_", false},

		// Escape
		{"", "\\", true},
		{"", "%\\", true},
		{"", "\\%", false},
		{"x", "%\\", true},
		{"x", "\\%", false},
		{"x", "_\\", true},
		{"x", "_\\x", false},
		{"%", "\\%", true},
		{"_", "\\_", true},
		{"x", "\\%", false},
		{"x", "\\_", false},
		{"x", "\\x", true},
		{"ab", "a\\", false},
		{"ab", "\\b", false},

		// Escaping escape
		{"", "\\\\", false},
		{"x", "\\\\", false},
		{"\\", "\\\\", true},
		{"\\", "%\\\\", true},
		{"\\", "\\\\%", true},
		{"\\", "_\\\\", false},
		{"\\", "\\\\_", false},
		{"x\\", "\\x\\", false},
		{"\\x", "\\\\x", true},

		// Exact
		{"abc", "abc", true},
		{"aBc", "AbC", true},
		{"abc", "def", false},

		// Case folding
		{"K", "\u212A", true}, // K → k → U+212A
		{"\u212A", "k", true},

		// Invalid UTF-8
		{"\xFF", "\xFF", true},
		{"\xFA", "\xFB", false},
		{"\xFF", "_", true},
		{"\xFF", "\xFF_", false},
		{"\xFF", "%", true},
		{"\xFF", "%\xFF%", true},
		{"\xFF", "x", false},

		// Prefix
		{"abc", "abc%", true},
		{"abcdef", "abc%", true},
		{"abcdef", "def%", false},

		// Suffix
		{"abc", "%abc", true},
		{"defabc", "%abc", true},
		{"defabc", "%def", false},

		// Contains
		{"defabcdef", "%abc%", true},
		{"abcd", "%def%", false},
		{"abc", "b", false},

		// Complex
		{"abc", "ab%d", false},
		{"ABCD", "%B%C%", true},
		{"ABxCxxD", "a%b%c%d", true},
		{"a", "__", false},
		{"ab", "__", true},
		{"abc", "___", true},
		{"abcd", "____", true},
		{"abc", "____", false},
		{"abcd", "_b__", true},
		{"abcd", "_a__", false},
		{"abcd", "__c_", true},
		{"abcd", "__d_", false},

		// Mixed
		{"", "%_", false},
		{"", "_%", false},
		{"a", "%_", true},
		{"a", "%__", false},
		{"ab", "%_", true},
		{"abc", "%_", true},
		{"ab", "_%_", true},
		{"ab", "%_%_%", true},
		{"aab", "%b_", false},
		{"aaaa", "_aa%", true},
		{"aaaa", "%aa_", true},
		{"abc", "_%%_%_", true},
		{"abc", "_%%_%&_", false},
		{"abcd", "_b%__", true},
		{"abcd", "_a%__", false},
		{"abcd", "_%%_c_", true},
		{"abcd", "_%%_d_", false},
		{"abcde", "_b_d%_", true},
		{"abcde", "_%b%_%d%_", true},
		{"abcd", "_%b%c%_", true},
		{"ABxCxxD", "%__B", false},
		{"abBbc", "%b_c", true},

		// Longer strings
		{
			"%abc%",
			"%%\\%a%b%c\\%%%",
			true,
		},
		{
			"aaabbaabbaab",
			"%aabbaa%a%",
			true,
		},
		{
			"abacaaadabacababacaaabadagabacaba",
			"%a%a%a%a%a%a%a%a%a%a%a%a%a%a%a%a%a%",
			true,
		},
		{
			"aaaaaaaaaaaaaaaa",
			"%a%a%a%a%a%a%a%a%a%a%a%a%a%a%a%a%a%",
			false,
		},
		{
			"%a%b%c%",
			"%%%%%%%%a%%%%\\%%%%b%%%%\\%%%%c%%%%%%%%",
			true,
		},
		{
			"a%a%a%a%a%a%a%a%a%a%a%a%a%a%a%a%a%",
			"a%a\\%a%a\\%a%a\\%a%a\\%a%a\\%a%a\\%a%a\\%a%a\\%a%",
			true,
		},
		{
			"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab",
			"a%a%a%a%a%a%aa%aaa%a%a%b",
			true,
		},
		{
			"abababababababababababababababababababaacacacacacacacadaeafagahaiajakalaaaaaaaaaaaaaaaaaffafagaagggagaaaaaaaab",
			"%a%b%ba%ca%a%aa%aaa%fa%ga%b%",
			true,
		},
		{
			"abababababababababababababababababababaacacacacacacacadaeafagahaiajakalaaaaaaaaaaaaaaaaaffafagaagggagaaaaaaaab",
			"%a%b%ba%ca%a%x%aaa%fa%ga%b%",
			false,
		},
		{
			"abababababababababababababababababababaacacacacacacacadaeafagahaiajakalaaaaaaaaaaaaaaaaaffafagaagggagaaaaaaaab",
			"%a%b%ba%ca%aaaa%fa%ga%gggg%b%",
			false,
		},
		{
			"abababababababababababababababababababaacacacacacacacadaeafagahaiajakalaaaaaaaaaaaaaaaaaffafagaagggagaaaaaaaab",
			"%a%b%ba%ca%aaaa%fa%ga%ggg%b%",
			true,
		},
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
