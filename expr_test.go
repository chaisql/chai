package genji

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOperator(t *testing.T) {
	tests := []struct {
		name  string
		fn    func(a, b expr) expr
		a, b  expr
		res   evalValue
		fails bool
	}{
		{"EQ / Same type", eq, int32Value(10), int32Value(10), trueLitteral, false},
		{"EQ / Same type / Different values", eq, int32Value(10), int32Value(11), falseLitteral, false},
		{"EQ / Numbers", eq, int32Value(10), uint64Value(10), trueLitteral, false},
		{"EQ / Numbers / Different values", eq, int32Value(10), uint64Value(11), falseLitteral, false},
		{"EQ / Numbers / List with one elem / Single value", eq,
			litteralExprList{int32Value(10)}, uint64Value(10), trueLitteral, false},
		{"EQ / Numbers / Single value / List with one elem", eq,
			uint64Value(10), litteralExprList{int32Value(10)}, trueLitteral, false},
		{"EQ / Lists", eq,
			litteralExprList{uint64Value(10), stringValue("foo")}, litteralExprList{int32Value(10), bytesValue([]byte("foo"))}, trueLitteral, false},
		{"EQ / Lists / Different lengths", eq,
			litteralExprList{uint64Value(10)}, litteralExprList{int32Value(10), bytesValue([]byte("foo"))}, falseLitteral, true},
		{"EQ / String Bytes", eq, stringValue("foo"), bytesValue([]byte("foo")), trueLitteral, false},
		{"EQ / Bytes String", eq, bytesValue([]byte("foo")), stringValue("foo"), trueLitteral, false},
		{"GT / Same type", gt, int32Value(11), int32Value(10), trueLitteral, false},
		{"GT / Same type / Lower", gt, int32Value(10), int32Value(11), falseLitteral, false},
		{"GT / Numbers", gt, int32Value(11), uint64Value(10), trueLitteral, false},
		{"GT / Numbers / Lower", gt, int32Value(10), uint64Value(11), falseLitteral, false},
		{"GT / String Bytes", gt, stringValue("foo2"), bytesValue([]byte("foo1")), trueLitteral, false},
		{"GT / Bytes String", gt, bytesValue([]byte("foo2")), stringValue("foo1"), trueLitteral, false},
		{"GTE / Same type", gte, int32Value(11), int32Value(10), trueLitteral, false},
		{"GTE / Same type / Lower", gte, int32Value(10), int32Value(11), falseLitteral, false},
		{"GTE / Numbers", gte, int32Value(11), uint64Value(10), trueLitteral, false},
		{"GTE / Numbers / Lower", gte, int32Value(10), uint64Value(11), falseLitteral, false},
		{"GTE / String Bytes", gte, stringValue("foo2"), bytesValue([]byte("foo1")), trueLitteral, false},
		{"GTE / Bytes String", gte, bytesValue([]byte("foo2")), stringValue("foo1"), trueLitteral, false},
		{"LT / Same type", lt, int32Value(10), int32Value(11), trueLitteral, false},
		{"LT / Same type / Greater", lt, int32Value(11), int32Value(10), falseLitteral, false},
		{"LT / Numbers", lt, int32Value(10), uint64Value(11), trueLitteral, false},
		{"LT / Numbers / Greater", lt, int32Value(11), uint64Value(10), falseLitteral, false},
		{"LT / String Bytes", lt, stringValue("foo1"), bytesValue([]byte("foo2")), trueLitteral, false},
		{"LT / Bytes String", lt, bytesValue([]byte("foo1")), stringValue("foo2"), trueLitteral, false},
		{"LTE / Same type", lte, int32Value(10), int32Value(11), trueLitteral, false},
		{"LTE / Same type / Greater", lte, int32Value(11), int32Value(10), falseLitteral, false},
		{"LTE / Numbers", lte, int32Value(10), uint64Value(11), trueLitteral, false},
		{"LTE / Numbers / Greater", lte, int32Value(11), uint64Value(10), falseLitteral, false},
		{"LTE / String Bytes", lte, stringValue("foo1"), bytesValue([]byte("foo2")), trueLitteral, false},
		{"LTE / Bytes String", lte, bytesValue([]byte("foo1")), stringValue("foo2"), trueLitteral, false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			res, err := test.fn(test.a, test.b).Eval(evalStack{})
			if test.fails {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, test.res, res)
		})
	}
}
