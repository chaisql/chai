//go:build go1.18
// +build go1.18

package parser

import (
	"testing"
)

func FuzzParseQuery(f *testing.F) {
	f.Fuzz(func(t *testing.T, s string) {
		// Fuzz ParseQuery for panics.
		statements, err := ParseQuery(s)
		if err != nil || len(statements) < 1 {
			t.Skip()
		}
	})
}
