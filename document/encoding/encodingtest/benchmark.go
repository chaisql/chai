package encodingtest

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/document/encoding"
	"github.com/genjidb/genji/internal/stringutil"
	"github.com/genjidb/genji/internal/testutil/assert"
	"github.com/genjidb/genji/types"
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
		fb.Add(stringutil.Sprintf("name-%d", i), types.NewIntegerValue(i))
	}

	codec := codecBuilder()

	d := types.NewDocumentValue(&fb)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var buf bytes.Buffer
		codec.EncodeValue(&buf, d)
	}
}

func benchmarkDocumentGetByField(b *testing.B, codecBuilder func() encoding.Codec) {
	var fb document.FieldBuffer

	for i := int64(0); i < 100; i++ {
		fb.Add(stringutil.Sprintf("name-%d", i), types.NewIntegerValue(i))
	}

	d := types.NewDocumentValue(&fb)

	codec := codecBuilder()
	var buf bytes.Buffer
	err := codec.EncodeValue(&buf, d)
	assert.NoError(b, err)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		v, _ := codec.DecodeValue(buf.Bytes())
		doc := v.V().(types.Document)
		doc.GetByField("name-99")
	}
}

func benchmarkDocumentIterate(b *testing.B, codecBuilder func() encoding.Codec) {
	var fb document.FieldBuffer

	for i := int64(0); i < 100; i++ {
		fb.Add(stringutil.Sprintf("name-%d", i), types.NewIntegerValue(i))
	}

	d := types.NewDocumentValue(&fb)

	codec := codecBuilder()
	var buf bytes.Buffer
	err := codec.EncodeValue(&buf, d)
	assert.NoError(b, err)

	v, _ := codec.DecodeValue(buf.Bytes())
	doc := v.V().(types.Document)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		doc.Iterate(func(string, types.Value) error {
			return nil
		})
	}
}

func benchmarkDecodeDocument(b *testing.B, codecBuilder func() encoding.Codec) {
	var fb document.FieldBuffer

	for i := int64(0); i < 100; i++ {
		fb.Add(stringutil.Sprintf("name-%d", i), types.NewIntegerValue(i))
	}

	d := types.NewDocumentValue(&fb)

	codec := codecBuilder()
	var buf bytes.Buffer
	err := codec.EncodeValue(&buf, d)
	assert.NoError(b, err)

	v, _ := codec.DecodeValue(buf.Bytes())
	doc := v.V().(types.Document)

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
	assert.NoError(b, err)

	mm := make(map[string]int64)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		json.Unmarshal(d, &mm)
	}
}
