package msgpack

import (
	"encoding/json"
	"fmt"
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

func BenchmarkEncodeDocument(b *testing.B) {
	var buf document.FieldBuffer

	for i := int64(0); i < 100; i++ {
		buf.Add(fmt.Sprintf("name-%d", i), document.NewIntegerValue(i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		EncodeDocument(&buf)
	}
}

func BenchmarkEncodeDocumentJSON(b *testing.B) {
	m := make(map[string]int64)

	for i := int64(0); i < 100; i++ {
		m[fmt.Sprintf("name-%d", i)] = i
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		json.Marshal(m)
	}
}

func BenchmarkGetByField(b *testing.B) {
	var buf document.FieldBuffer

	for i := int64(0); i < 100; i++ {
		buf.Add(fmt.Sprintf("name-%d", i), document.NewIntegerValue(i))
	}

	data, err := EncodeDocument(&buf)
	require.NoError(b, err)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		DecodeDocument(data).GetByField("name-99")
	}
}

func BenchmarkDocumentIterate(b *testing.B) {
	var buf document.FieldBuffer

	for i := int64(0); i < 100; i++ {
		buf.Add(fmt.Sprintf("name-%d", i), document.NewIntegerValue(i))
	}

	data, err := EncodeDocument(&buf)
	require.NoError(b, err)

	ec := DecodeDocument(data)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ec.Iterate(func(string, document.Value) error {
			return nil
		})
	}
}

func BenchmarkDecodeDocumentJSON(b *testing.B) {
	m := make(map[string]int64)

	for i := int64(0); i < 100; i++ {
		m[fmt.Sprintf("name-%d", i)] = i
	}

	d, err := json.Marshal(m)
	require.NoError(b, err)

	mm := make(map[string]int64)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		json.Unmarshal(d, &mm)
	}
}
