package document_test

import (
	"testing"

	"github.com/asdine/genji/document"
	"github.com/stretchr/testify/require"
)

func TestFieldString(t *testing.T) {
	tests := []struct {
		name     string
		field    document.Field
		expected string
	}{
		{"bytes", document.NewBytesField("foo", []byte("bar")), "foo:[98 97 114]"},
		{"string", document.NewStringField("foo", "bar"), "foo:bar"},
		{"bool", document.NewBoolField("foo", true), "foo:true"},
		{"uint", document.NewUintField("foo", 10), "foo:10"},
		{"uint8", document.NewUint8Field("foo", 10), "foo:10"},
		{"uint16", document.NewUint16Field("foo", 10), "foo:10"},
		{"uint32", document.NewUint32Field("foo", 10), "foo:10"},
		{"uint64", document.NewUint64Field("foo", 10), "foo:10"},
		{"int", document.NewIntField("foo", 10), "foo:10"},
		{"int8", document.NewInt8Field("foo", 10), "foo:10"},
		{"int16", document.NewInt16Field("foo", 10), "foo:10"},
		{"int32", document.NewInt32Field("foo", 10), "foo:10"},
		{"int64", document.NewInt64Field("foo", 10), "foo:10"},
		{"float64", document.NewFloat64Field("foo", 10.1), "foo:10.1"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.expected, test.field.String())
		})
	}
}
