package encoding_test

import (
	"bytes"
	"testing"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/internal/stringutil"
	"github.com/genjidb/genji/internal/testutil/assert"
	"github.com/genjidb/genji/types"
	"github.com/genjidb/genji/types/encoding"
)

func BenchmarkEncodeDocument(b *testing.B) {
	var fb document.FieldBuffer

	for i := int64(0); i < 100; i++ {
		fb.Add(stringutil.Sprintf("name-%d", i), types.NewIntegerValue(i))
	}

	d := types.NewDocumentValue(&fb)
	var buf bytes.Buffer
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf.Reset()
		encoding.EncodeValue(&buf, d)
	}
}

func BenchmarkDocumentGetByField(b *testing.B) {
	var fb document.FieldBuffer

	for i := int64(0); i < 100; i++ {
		fb.Add(stringutil.Sprintf("name-%d", i), types.NewIntegerValue(i))
	}

	d := types.NewDocumentValue(&fb)

	var buf bytes.Buffer
	err := encoding.EncodeValue(&buf, d)
	assert.NoError(b, err)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		v, _ := encoding.DecodeValue(buf.Bytes())
		doc := v.V().(types.Document)
		doc.GetByField("name-99")
	}
}

func BenchmarkDocumentIterate(b *testing.B) {
	var fb document.FieldBuffer

	for i := int64(0); i < 100; i++ {
		fb.Add(stringutil.Sprintf("name-%d", i), types.NewIntegerValue(i))
	}

	d := types.NewDocumentValue(&fb)

	var buf bytes.Buffer
	err := encoding.EncodeValue(&buf, d)
	assert.NoError(b, err)

	v, _ := encoding.DecodeValue(buf.Bytes())
	doc := v.V().(types.Document)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		doc.Iterate(func(string, types.Value) error {
			return nil
		})
	}
}

func BenchmarkDecodeDocument(b *testing.B) {
	var fb document.FieldBuffer

	for i := int64(0); i < 100; i++ {
		fb.Add(stringutil.Sprintf("bool-%d", i), types.NewBoolValue(true))
		fb.Add(stringutil.Sprintf("int-%d", i), types.NewIntegerValue(i))
		fb.Add(stringutil.Sprintf("double-%d", i), types.NewDoubleValue(float64(i)))
		fb.Add(stringutil.Sprintf("text-%d", i), types.NewTextValue(stringutil.Sprintf("name-%d", i)))
		fb.Add(stringutil.Sprintf("blob-%d", i), types.NewBlobValue([]byte(stringutil.Sprintf("blob-%d", i))))
		fb.Add(stringutil.Sprintf("array-%d", i), types.NewArrayValue(document.NewValueBuffer(
			types.NewBoolValue(true),
			types.NewIntegerValue(i),
			types.NewDoubleValue(float64(i)),
		)))
		fb.Add(stringutil.Sprintf("document-%d", i), types.NewDocumentValue(document.NewFieldBuffer().
			Add("bool", types.NewBoolValue(true)).
			Add("int", types.NewIntegerValue(i)).
			Add("double", types.NewDoubleValue(float64(i))),
		))
	}

	d := types.NewDocumentValue(&fb)

	var buf bytes.Buffer
	err := encoding.EncodeValue(&buf, d)
	assert.NoError(b, err)

	v, _ := encoding.DecodeValue(buf.Bytes())

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		walkValue(v)
	}
}

func walkValue(v types.Value) {
	switch v.Type() {
	case types.ArrayValue:
		v.V().(types.Array).Iterate(func(i int, value types.Value) error {
			walkValue(value)
			return nil
		})
	case types.DocumentValue:
		v.V().(types.Document).Iterate(func(field string, value types.Value) error {
			walkValue(value)
			return nil
		})
	default:
		v.V()
	}
}
