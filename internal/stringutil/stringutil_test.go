package stringutil

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNeedsQuote(t *testing.T) {
	tests := []struct {
		s           string
		needsQuotes bool
	}{
		{"0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ_", false},
		{"abc ", true},
		{"'", true},
		{"", true},
	}

	for _, test := range tests {
		t.Run(test.s, func(t *testing.T) {
			require.Equal(t, test.needsQuotes, NeedsQuotes(test.s))
		})
	}
}
