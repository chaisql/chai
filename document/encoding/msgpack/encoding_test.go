package msgpack_test

import (
	"bytes"
	"testing"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/document/encoding"
	"github.com/genjidb/genji/document/encoding/encodingtest"
	"github.com/genjidb/genji/document/encoding/msgpack"
	"github.com/genjidb/genji/internal/testutil"
	"github.com/genjidb/genji/internal/testutil/assert"
	"github.com/genjidb/genji/types"
)

func TestCodec(t *testing.T) {
	encodingtest.TestCodec(t, func() encoding.Codec {
		return msgpack.NewCodec()
	})
}

// The codec decoder has a specific branch to handle the case where low value integers are encoded
// on a single byte by msgpack, with 7bit for positive uint8 and 5bit for negative int8.
func TestCompactedIntDecoding(t *testing.T) {
	d := document.NewFieldBuffer().
		Add("small-pos-int", types.NewIntegerValue(127)).
		Add("smaller-pos-int", types.NewIntegerValue(2)).
		Add("small-neg-int", types.NewIntegerValue(-2)).
		Add("smaller-neg-int", types.NewIntegerValue(-32)).
		Add("normal-pos-int", types.NewIntegerValue(2048)).
		Add("normal-neg-int", types.NewIntegerValue(-2048))

	codec := msgpack.NewCodec()
	var buf bytes.Buffer

	err := codec.EncodeValue(&buf, types.NewDocumentValue(d))
	assert.NoError(t, err)

	doc, err := codec.DecodeValue(buf.Bytes())
	assert.NoError(t, err)

	testutil.RequireDocEqual(t, d, doc.V().(types.Document))
}

func BenchmarkCodec(b *testing.B) {
	encodingtest.BenchmarkCodec(b, func() encoding.Codec {
		return msgpack.NewCodec()
	})
}
