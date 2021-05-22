package encodingtest

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/document/encoding"
	"github.com/genjidb/genji/internal/stringutil"
	"github.com/stretchr/testify/require"
)

// BenchmarkCodec runs benchmarks against the given codec.
func BenchmarkCodec(b *testing.B, codecBuilder func() encoding.Codec) {
	benchmarks := []struct {
		name string
		test func(*testing.B, func() encoding.Codec)
	}{
		{"Codec/Encode", benchmarkEncodeDocument},
		{"Codec/Decode", benchmarkDecodeDocument},
		{"Codec/Document/GetByField", benchmarkDocumentGetByField},
		{"Codec/Document/Iterate", benchmarkDocumentIterate},
		{"ComparedWithJSON/Encode", benchmarkEncodeDocumentJSON},
		{"ComparedWithJSON/Decode", benchmarkDecodeDocumentJSON},
	}

	for _, test := range benchmarks {
		b.Run(test.name, func(b *testing.B) {
			test.test(b, codecBuilder)
		})
	}
}

func benchmarkEncodeDocument(b *testing.B, codecBuilder func() encoding.Codec) {
	var fb document.FieldBuffer

	for i := int64(0); i < 100; i++ {
		fb.Add(stringutil.Sprintf("name-%d", i), document.NewIntegerValue(i))
	}

	codec := codecBuilder()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var buf bytes.Buffer
		enc := codec.NewEncoder(&buf)
		enc.EncodeDocument(&fb)
		enc.Close()
	}
}

func benchmarkDocumentGetByField(b *testing.B, codecBuilder func() encoding.Codec) {
	var fb document.FieldBuffer

	for i := int64(0); i < 100; i++ {
		fb.Add(stringutil.Sprintf("name-%d", i), document.NewIntegerValue(i))
	}

	codec := codecBuilder()
	var buf bytes.Buffer
	err := codec.NewEncoder(&buf).EncodeDocument(&fb)
	require.NoError(b, err)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		codec.NewDocument(buf.Bytes()).GetByField("name-99")
	}
}

func benchmarkDocumentIterate(b *testing.B, codecBuilder func() encoding.Codec) {
	var fb document.FieldBuffer

	for i := int64(0); i < 100; i++ {
		fb.Add(stringutil.Sprintf("name-%d", i), document.NewIntegerValue(i))
	}

	codec := codecBuilder()
	var buf bytes.Buffer
	err := codec.NewEncoder(&buf).EncodeDocument(&fb)
	require.NoError(b, err)

	doc := codec.NewDocument(buf.Bytes())

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		doc.Iterate(func(string, document.Value) error {
			return nil
		})
	}
}

func benchmarkDecodeDocument(b *testing.B, codecBuilder func() encoding.Codec) {
	var fb document.FieldBuffer

	for i := int64(0); i < 100; i++ {
		fb.Add(stringutil.Sprintf("name-%d", i), document.NewIntegerValue(i))
	}

	codec := codecBuilder()
	var buf bytes.Buffer
	err := codec.NewEncoder(&buf).EncodeDocument(&fb)
	require.NoError(b, err)

	doc := codec.NewDocument(buf.Bytes())

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var buf document.FieldBuffer
		buf.Copy(doc)
	}
}

func benchmarkEncodeDocumentJSON(b *testing.B, codecBuilder func() encoding.Codec) {
	m := make(map[string]int64)

	for i := int64(0); i < 100; i++ {
		m[stringutil.Sprintf("name-%d", i)] = i
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		json.Marshal(m)
	}
}

func benchmarkDecodeDocumentJSON(b *testing.B, codecBuilder func() encoding.Codec) {
	m := make(map[string]int64)

	for i := int64(0); i < 100; i++ {
		m[stringutil.Sprintf("name-%d", i)] = i
	}

	d, err := json.Marshal(m)
	require.NoError(b, err)

	mm := make(map[string]int64)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		json.Unmarshal(d, &mm)
	}
}
