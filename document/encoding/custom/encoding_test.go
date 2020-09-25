package custom

import (
	"testing"

	"github.com/genjidb/genji/document/encoding"
	"github.com/genjidb/genji/document/encoding/encodingtest"
)

func TestCodec(t *testing.T) {
	encodingtest.TestCodec(t, func() encoding.Codec {
		return NewCodec()
	})
}

func BenchmarkCodec(b *testing.B) {
	encodingtest.BenchmarkCodec(b, func() encoding.Codec {
		return NewCodec()
	})
}
