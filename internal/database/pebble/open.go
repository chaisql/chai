package pebble

import (
	"fmt"

	"github.com/cockroachdb/pebble"
	"github.com/genjidb/genji/internal/encoding"
)

// Open a database with a custom comparer.
func Open(path string, opts *pebble.Options) (*pebble.DB, error) {
	if opts == nil {
		opts = &pebble.Options{}
	}

	if opts.Comparer == nil {
		opts.Comparer = DefaultComparer
	}
	return pebble.Open(path, opts)
}

// DefaultComparer is the default implementation of the Comparer interface for Genji.
var DefaultComparer = &pebble.Comparer{
	Compare:        encoding.Compare,
	Equal:          encoding.Equal,
	AbbreviatedKey: encoding.AbbreviatedKey,
	FormatKey:      pebble.DefaultComparer.FormatKey,
	Separator:      pebble.DefaultComparer.Separator,
	Successor:      encoding.Successor,
	// This name is part of the C++ Level-DB implementation's default file
	// format, and should not be changed.
	Name: "leveldb.BytewiseComparator",
}

var stats = struct {
	SuccessorCount int
	SeparatorCount int
	CompareCount   int
	EqualCount     int
	AbbrevCount    int
	FormatKeyCount int
}{}

// DefaultComparer is the default implementation of the Comparer interface for Genji.
func WithStats(cmp *pebble.Comparer) *pebble.Comparer {
	return &pebble.Comparer{
		Compare: func(a, b []byte) int {
			stats.CompareCount++
			return cmp.Compare(a, b)
		},
		Equal: func(a, b []byte) bool {
			stats.EqualCount++
			return cmp.Equal(a, b)
		},
		AbbreviatedKey: func(key []byte) uint64 {
			stats.AbbrevCount++
			return cmp.AbbreviatedKey(key)
		},
		FormatKey: func(key []byte) fmt.Formatter {
			stats.FormatKeyCount++
			return cmp.FormatKey(key)
		},
		Separator: func(dst, a, b []byte) []byte {
			stats.SeparatorCount++
			return cmp.Separator(dst, a, b)
		},
		Successor: func(dst, a []byte) []byte {
			stats.SuccessorCount++
			return cmp.Successor(dst, a)
		},
		// This name is part of the C++ Level-DB implementation's default file
		// format, and should not be changed.
		Name: "leveldb.BytewiseComparator",
	}
}
