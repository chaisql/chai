//go:build go1.17
// +build go1.17

package parser

import (
	"testing"
)

func FuzzParseQuery(f *testing.F) {
	f.Fuzz(func(t *testing.T, s string) {
		// Fuzz ParseQuery for panics.
		q, err := ParseQuery(s)
		if err != nil || len(q.Statements) < 1 {
			t.Skip()
		}
	})
}
