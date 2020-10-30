package key

import (
	"testing"

	"github.com/genjidb/genji/document"
	"github.com/stretchr/testify/require"
)

func TestAppendDecode(t *testing.T) {
	tests := []struct {
		name string
		v    document.Value
	}{
		{"null", document.NewNullValue()},
		{"bool", document.NewBoolValue(true)},
		{"integer", document.NewIntegerValue(-10)},
		{"double", document.NewDoubleValue(-3.14)},
		{"text", document.NewTextValue("foo")},
		{"blob", document.NewBlobValue([]byte("bar"))},
		{"array", document.NewArrayValue(document.NewValueBuffer(
			document.NewBoolValue(true),
			document.NewIntegerValue(55),
			document.NewDoubleValue(789.58),
			document.NewArrayValue(document.NewValueBuffer(
				document.NewBoolValue(false),
				document.NewIntegerValue(100),
				document.NewTextValue("baz"),
			)),
			document.NewBlobValue([]byte("loo")),
			document.NewDocumentValue(
				document.NewFieldBuffer().
					Add("foo1", document.NewBoolValue(true)).
					Add("foo2", document.NewIntegerValue(55)).
					Add("foo3", document.NewArrayValue(document.NewValueBuffer(
						document.NewBoolValue(false),
						document.NewIntegerValue(100),
						document.NewTextValue("baz"),
					))),
			),
		))},
		{"document", document.NewDocumentValue(
			document.NewFieldBuffer().
				Add("foo1", document.NewBoolValue(true)).
				Add("foo2", document.NewIntegerValue(55)).
				Add("foo3", document.NewArrayValue(document.NewValueBuffer(
					document.NewBoolValue(false),
					document.NewIntegerValue(100),
					document.NewTextValue("baz"),
				))).
				Add("foo4", document.NewDocumentValue(
					document.NewFieldBuffer().
						Add("foo1", document.NewBoolValue(true)).
						Add("foo2", document.NewIntegerValue(55)).
						Add("foo3", document.NewArrayValue(document.NewValueBuffer(
							document.NewBoolValue(false),
							document.NewIntegerValue(100),
							document.NewTextValue("baz"),
						))),
				)),
		)},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			b, err := Append(nil, test.v.Type, test.v.V)
			require.NoError(t, err)

			got, err := Decode(test.v.Type, b)
			require.NoError(t, err)
			require.Equal(t, test.v, got)
		})
	}
}

func TestAppendValueDecodeValue(t *testing.T) {
	tests := []struct {
		name string
		v    document.Value
	}{
		{"null", document.NewNullValue()},
		{"bool", document.NewBoolValue(true)},
		{"integer", document.NewIntegerValue(-10)},
		{"double", document.NewDoubleValue(-3.14)},
		{"text", document.NewTextValue("foo")},
		{"blob", document.NewBlobValue([]byte("bar"))},
		{"array", document.NewArrayValue(document.NewValueBuffer(
			document.NewBoolValue(true),
			document.NewIntegerValue(55),
			document.NewDoubleValue(789.58),
			document.NewArrayValue(document.NewValueBuffer(
				document.NewBoolValue(false),
				document.NewIntegerValue(100),
				document.NewTextValue("baz"),
			)),
			document.NewBlobValue([]byte("loo")),
			document.NewDocumentValue(
				document.NewFieldBuffer().
					Add("foo1", document.NewBoolValue(true)).
					Add("foo2", document.NewIntegerValue(55)).
					Add("foo3", document.NewArrayValue(document.NewValueBuffer(
						document.NewBoolValue(false),
						document.NewIntegerValue(100),
						document.NewTextValue("baz"),
					))),
			),
		))},
		{"document", document.NewDocumentValue(
			document.NewFieldBuffer().
				Add("foo1", document.NewBoolValue(true)).
				Add("foo2", document.NewIntegerValue(55)).
				Add("foo3", document.NewArrayValue(document.NewValueBuffer(
					document.NewBoolValue(false),
					document.NewIntegerValue(100),
					document.NewTextValue("baz"),
				))).
				Add("foo4", document.NewDocumentValue(
					document.NewFieldBuffer().
						Add("foo1", document.NewBoolValue(true)).
						Add("foo2", document.NewIntegerValue(55)).
						Add("foo3", document.NewArrayValue(document.NewValueBuffer(
							document.NewBoolValue(false),
							document.NewIntegerValue(100),
							document.NewTextValue("baz"),
						))),
				)),
		)},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			b, err := AppendValue(nil, test.v)
			require.NoError(t, err)

			got, err := DecodeValue(b)
			require.NoError(t, err)
			require.Equal(t, test.v, got)
		})
	}
}
