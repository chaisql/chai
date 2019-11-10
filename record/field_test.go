package record_test

import (
	"testing"

	"github.com/asdine/genji/record"
	"github.com/stretchr/testify/require"
)

func TestFieldString(t *testing.T) {
	tests := []struct {
		name     string
		field    record.Field
		expected string
	}{
		{"bytes", record.NewBytesField("foo", []byte("bar")), "foo:[98 97 114]"},
		{"string", record.NewStringField("foo", "bar"), "foo:bar"},
		{"bool", record.NewBoolField("foo", true), "foo:true"},
		{"uint", record.NewUintField("foo", 10), "foo:10"},
		{"uint8", record.NewUint8Field("foo", 10), "foo:10"},
		{"uint16", record.NewUint16Field("foo", 10), "foo:10"},
		{"uint32", record.NewUint32Field("foo", 10), "foo:10"},
		{"uint64", record.NewUint64Field("foo", 10), "foo:10"},
		{"int", record.NewIntField("foo", 10), "foo:10"},
		{"int8", record.NewInt8Field("foo", 10), "foo:10"},
		{"int16", record.NewInt16Field("foo", 10), "foo:10"},
		{"int32", record.NewInt32Field("foo", 10), "foo:10"},
		{"int64", record.NewInt64Field("foo", 10), "foo:10"},
		{"float64", record.NewFloat64Field("foo", 10.1), "foo:10.1"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.expected, test.field.String())
		})
	}
}
