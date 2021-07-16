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
	NewEncoder(io.Writer) Encoder
	// NewDocument returns a document without decoding its given binary representation.
	// The returned document should ideally support random-access, i.e. decoding one path
	// without decoding the entire document. If not, the document must be lazily decoded.
	NewDecoder([]byte) Decoder
}

// An Encoder encodes one document to the underlying writer.
type Encoder interface {
	EncodeDocument(d types.Document) error
	// Close the encoder to release any resource.
	Close()
}

// A Decoder represents an encoded document that can
// be used as if it was decoded.
// Decoders can be reused to read different documents.
type Decoder interface {
	types.Document

	Reset([]byte)
}
