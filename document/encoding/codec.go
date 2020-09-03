// Package encoding defines types that deal with document encoding.
// Genji codecs are designed to support buffer reuse during encoding
// and optional random-access, i.e. decoding one path without decoding the entire document,
// during decoding.
package encoding

import (
	"io"

	"github.com/genjidb/genji/document"
)

// A Codec is able to create encoders and decoders for a specific encoding format.
type Codec interface {
	NewEncoder(io.Writer) Encoder
	// NewDocument returns a document without decoding its given binary representation.
	// The returned document should ideally support random-access, i.e. decoding one path
	// without decoding the entire document. If not, the document must be lazily decoded.
	NewDocument([]byte) document.Document
}

// An Encoder encodes one document to the underlying writer.
type Encoder interface {
	EncodeDocument(d document.Document) error
}
