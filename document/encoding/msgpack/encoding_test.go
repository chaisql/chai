package msgpack

import (
	"bytes"
	"testing"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/document/encoding"
	"github.com/genjidb/genji/document/encoding/encodingtest"
	"github.com/stretchr/testify/require"
)

func TestCodec(t *testing.T) {
	encodingtest.TestCodec(t, func() encoding.Codec {
		return NewCodec()
	})
}

// The codec decoder has a specific branch to handle the case where low value integers are encoded
// on a single byte by msgpack, with 7bit for positive uint8 and 5bit for negative int8.
func TestCompactedIntDecoding(t *testing.T) {
	d := document.NewFieldBuffer().
		Add("small-pos-int", document.NewIntegerValue(127)).
		Add("smaller-pos-int", document.NewIntegerValue(2)).
		Add("small-neg-int", document.NewIntegerValue(-2)).
		Add("smaller-neg-int", document.NewIntegerValue(-32)).
		Add("normal-pos-int", document.NewIntegerValue(2048)).
		Add("normal-neg-int", document.NewIntegerValue(-2048))

	expected := `{"small-pos-int": 127, "smaller-pos-int": 2, "small-neg-int": -2, "smaller-neg-int": -32, "normal-pos-int": 2048, "normal-neg-int": -2048}`

	codec := NewCodec()
	var buf bytes.Buffer

	err := codec.NewEncoder(&buf).EncodeDocument(d)
	require.NoError(t, err)

	doc := codec.NewDecoder(buf.Bytes())
	data, err := document.MarshalJSON(doc)
	require.NoError(t, err)
	require.JSONEq(t, expected, string(data))
}

func BenchmarkCodec(b *testing.B) {
	encodingtest.BenchmarkCodec(b, func() encoding.Codec {
		return NewCodec()
	})
}
