// Package encoding defines types that deal with document encoding.
// Genji codecs are designed to support buffer reuse during encoding
// and optional random-access, i.e. decoding one path without decoding the entire document,
// during decoding.
package encoding

import (
	"io"

	"github.com/genjidb/genji/types"
)

// A Codec is able to create encoders and decoders for a specific encoding format.
type Codec interface {
	EncodeValue(io.Writer, types.Value) error
	DecodeValue([]byte) (types.Value, error)
}
