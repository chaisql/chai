package encoding_test

import (
	"bytes"
	"testing"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/internal/testutil/assert"
	"github.com/genjidb/genji/types"
	"github.com/genjidb/genji/types/encoding"
	"github.com/stretchr/testify/require"
)

func TestValueEncoder(t *testing.T) {
	tests := []struct {
		name string
		v    types.Value
	}{
		{"null", types.NewNullValue()},
		{"bool", types.NewBoolValue(true)},
		{"integer", types.NewIntegerValue(-10)},
		{"double", types.NewDoubleValue(-3.14)},
		{"text", types.NewTextValue("foo")},
		{"blob", types.NewBlobValue([]byte("bar"))},
		{"array", types.NewArrayValue(document.NewValueBuffer(
			types.NewBoolValue(true),
			types.NewIntegerValue(55),
			types.NewDoubleValue(789.58),
			types.NewArrayValue(document.NewValueBuffer(
				types.NewBoolValue(false),
				types.NewIntegerValue(100),
				types.NewTextValue("baz"),
			)),
			types.NewBlobValue([]byte("loo")),
			types.NewDocumentValue(
				document.NewFieldBuffer().
					Add("foo1", types.NewBoolValue(true)).
					Add("foo2", types.NewIntegerValue(55)).
					Add("foo3", types.NewArrayValue(document.NewValueBuffer(
						types.NewBoolValue(false),
						types.NewIntegerValue(100),
						types.NewTextValue("baz"),
					))),
			),
		))},
		{"document", types.NewDocumentValue(
			document.NewFieldBuffer().
				Add("foo1", types.NewBoolValue(true)).
				Add("foo2", types.NewIntegerValue(55)).
				Add("foo3", types.NewArrayValue(document.NewValueBuffer(
					types.NewBoolValue(false),
					types.NewIntegerValue(100),
					types.NewTextValue("baz"),
				))).
				Add("foo4", types.NewDocumentValue(
					document.NewFieldBuffer().
						Add("foo1", types.NewBoolValue(true)).
						Add("foo2", types.NewIntegerValue(55)).
						Add("foo3", types.NewArrayValue(document.NewValueBuffer(
							types.NewBoolValue(false),
							types.NewIntegerValue(100),
							types.NewTextValue("baz"),
						))),
				)),
		)},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var buf bytes.Buffer

			enc := encoding.NewValueEncoder(&buf)
			err := enc.Encode(test.v)
			assert.NoError(t, err)

			got, err := encoding.DecodeValue(buf.Bytes())
			assert.NoError(t, err)
			require.Equal(t, test.v, got)
		})
	}
}
