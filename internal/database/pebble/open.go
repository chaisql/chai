package pebble

import (
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
	Separator:      encoding.Separator,
	Successor:      encoding.Successor,
	// This name is part of the C++ Level-DB implementation's default file
	// format, and should not be changed.
	Name: "leveldb.BytewiseComparator",
}
