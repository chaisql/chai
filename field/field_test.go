package field_test

import (
	"testing"

	"github.com/asdine/genji/field"
	"github.com/stretchr/testify/require"
)

func TestFieldString(t *testing.T) {
	tests := []struct {
		name     string
		field    field.Field
		expected string
	}{
		{"bytes", field.NewBytes("foo", []byte("bar")), "foo:[98 97 114]"},
		{"string", field.NewString("foo", "bar"), "foo:bar"},
		{"bool", field.NewBool("foo", true), "foo:true"},
		{"uint", field.NewUint("foo", 10), "foo:10"},
		{"uint8", field.NewUint8("foo", 10), "foo:10"},
		{"uint16", field.NewUint16("foo", 10), "foo:10"},
		{"uint32", field.NewUint32("foo", 10), "foo:10"},
		{"uint64", field.NewUint64("foo", 10), "foo:10"},
		{"int", field.NewInt("foo", 10), "foo:10"},
		{"int8", field.NewInt8("foo", 10), "foo:10"},
		{"int16", field.NewInt16("foo", 10), "foo:10"},
		{"int32", field.NewInt32("foo", 10), "foo:10"},
		{"int64", field.NewInt64("foo", 10), "foo:10"},
		{"float32", field.NewFloat32("foo", 10.1), "foo:10.1"},
		{"float64", field.NewFloat64("foo", 10.1), "foo:10.1"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.expected, test.field.String())
		})
	}
}
