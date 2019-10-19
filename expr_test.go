package genji

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOperator(t *testing.T) {
	tests := []struct {
		name string
		fn   func(a, b expr) expr
		a, b expr
		res  evalValue
	}{
		{"EQ / Same type", eq, int32Value(10), int32Value(10), trueLitteral},
		{"EQ / Same type / Different values", eq, int32Value(10), int32Value(11), falseLitteral},
		{"EQ / Numbers", eq, int32Value(10), uint64Value(10), trueLitteral},
		{"EQ / Numbers / Different values", eq, int32Value(10), uint64Value(11), falseLitteral},
		{"EQ / String Bytes", eq, stringValue("foo"), bytesValue([]byte("foo")), trueLitteral},
		{"EQ / Bytes String", eq, bytesValue([]byte("foo")), stringValue("foo"), trueLitteral},
		{"GT / Same type", gt, int32Value(11), int32Value(10), trueLitteral},
		{"GT / Same type / Lower", gt, int32Value(10), int32Value(11), falseLitteral},
		{"GT / Numbers", gt, int32Value(11), uint64Value(10), trueLitteral},
		{"GT / Numbers / Lower", gt, int32Value(10), uint64Value(11), falseLitteral},
		{"GT / String Bytes", gt, stringValue("foo2"), bytesValue([]byte("foo1")), trueLitteral},
		{"GT / Bytes String", gt, bytesValue([]byte("foo2")), stringValue("foo1"), trueLitteral},
		{"GTE / Same type", gte, int32Value(11), int32Value(10), trueLitteral},
		{"GTE / Same type / Lower", gte, int32Value(10), int32Value(11), falseLitteral},
		{"GTE / Numbers", gte, int32Value(11), uint64Value(10), trueLitteral},
		{"GTE / Numbers / Lower", gte, int32Value(10), uint64Value(11), falseLitteral},
		{"GTE / String Bytes", gte, stringValue("foo2"), bytesValue([]byte("foo1")), trueLitteral},
		{"GTE / Bytes String", gte, bytesValue([]byte("foo2")), stringValue("foo1"), trueLitteral},
		{"LT / Same type", lt, int32Value(10), int32Value(11), trueLitteral},
		{"LT / Same type / Greater", lt, int32Value(11), int32Value(10), falseLitteral},
		{"LT / Numbers", lt, int32Value(10), uint64Value(11), trueLitteral},
		{"LT / Numbers / Greater", lt, int32Value(11), uint64Value(10), falseLitteral},
		{"LT / String Bytes", lt, stringValue("foo1"), bytesValue([]byte("foo2")), trueLitteral},
		{"LT / Bytes String", lt, bytesValue([]byte("foo1")), stringValue("foo2"), trueLitteral},
		{"LTE / Same type", lte, int32Value(10), int32Value(11), trueLitteral},
		{"LTE / Same type / Greater", lte, int32Value(11), int32Value(10), falseLitteral},
		{"LTE / Numbers", lte, int32Value(10), uint64Value(11), trueLitteral},
		{"LTE / Numbers / Greater", lte, int32Value(11), uint64Value(10), falseLitteral},
		{"LTE / String Bytes", lte, stringValue("foo1"), bytesValue([]byte("foo2")), trueLitteral},
		{"LTE / Bytes String", lte, bytesValue([]byte("foo1")), stringValue("foo2"), trueLitteral},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			res, err := test.fn(test.a, test.b).Eval(evalStack{})
			require.NoError(t, err)
			require.Equal(t, test.res, res)
		})
	}
}
