package genji

import (
	"math"
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
		{"EQ / Numbers", eq, int32Value(10), float64Value(10), trueLitteral, false},
		{"EQ / Numbers / Different values", eq, int32Value(10), uint64Value(11), falseLitteral, false},
		{"EQ / Numbers / List with one elem / Single value", eq,
			litteralExprList{int32Value(10)}, uint64Value(10), trueLitteral, false},
		{"EQ / Numbers / Single value / List with one elem", eq,
			uint64Value(10), litteralExprList{int32Value(10)}, trueLitteral, false},
		{"EQ / Lists", eq,
			litteralExprList{uint64Value(10), stringValue("foo")}, litteralExprList{int32Value(10), bytesValue([]byte("foo"))}, trueLitteral, false},
		{"EQ / Lists / Different lengths", eq,
			litteralExprList{uint64Value(10)}, litteralExprList{int32Value(10), bytesValue([]byte("foo"))}, falseLitteral, true},
		{"EQ / Null", eq, nilLitteral.Value, nilLitteral.Value, trueLitteral, false},
		{"EQ / Zero and Null", eq, intValue(0), nilLitteral.Value, falseLitteral, false},
		{"EQ / String Bytes", eq, stringValue("foo"), bytesValue([]byte("foo")), trueLitteral, false},
		{"EQ / Bytes String", eq, bytesValue([]byte("foo")), stringValue("foo"), trueLitteral, false},
		{"GT / Same type", gt, int32Value(11), int32Value(10), trueLitteral, false},
		{"GT / Same type / Lower", gt, int32Value(10), int32Value(11), falseLitteral, false},
		{"GT / Numbers", gt, int32Value(11), uint64Value(10), trueLitteral, false},
		{"GT / Numbers / Lower", gt, int32Value(10), uint64Value(11), falseLitteral, false},
		{"GT / Null", gt, nilLitteral.Value, nilLitteral.Value, falseLitteral, false},
		{"GT / Zero and Null", gt, stringValue(""), nilLitteral.Value, falseLitteral, false},
		{"GT / Null and Zero", gt, nilLitteral.Value, stringValue(""), falseLitteral, false},
		{"GT / Value and Null", gt, stringValue("foo"), nilLitteral.Value, falseLitteral, false},
		{"GT / Null and Value", gt, nilLitteral.Value, stringValue("foo"), falseLitteral, false},
		{"GT / String Bytes", gt, stringValue("foo2"), bytesValue([]byte("foo1")), trueLitteral, false},
		{"GT / Bytes String", gt, bytesValue([]byte("foo2")), stringValue("foo1"), trueLitteral, false},
		{"GT / Numbers / Different sizes", gt, uint64Value(math.MaxUint64), int64Value(math.MaxInt64), trueLitteral, false},
		{"GTE / Same type", gte, int32Value(11), int32Value(10), trueLitteral, false},
		{"GTE / Same type / Lower", gte, int32Value(10), int32Value(11), falseLitteral, false},
		{"GTE / Numbers", gte, int32Value(11), uint64Value(10), trueLitteral, false},
		{"GTE / Numbers / Lower", gte, int32Value(10), uint64Value(11), falseLitteral, false},
		{"GTE / Null", gte, nilLitteral.Value, nilLitteral.Value, trueLitteral, false},
		{"GTE / Zero and Null", gte, stringValue(""), nilLitteral.Value, falseLitteral, false},
		{"GTE / Null and Zero", gte, nilLitteral.Value, stringValue(""), falseLitteral, false},
		{"GTE / Value and Null", gte, stringValue("foo"), nilLitteral.Value, falseLitteral, false},
		{"GTE / Null and Value", gte, nilLitteral.Value, stringValue("foo"), falseLitteral, false},
		{"GTE / String Bytes", gte, stringValue("foo2"), bytesValue([]byte("foo1")), trueLitteral, false},
		{"GTE / Bytes String", gte, bytesValue([]byte("foo2")), stringValue("foo1"), trueLitteral, false},
		{"LT / Same type", lt, int32Value(10), int32Value(11), trueLitteral, false},
		{"LT / Same type / Greater", lt, int32Value(11), int32Value(10), falseLitteral, false},
		{"LT / Numbers", lt, int32Value(10), uint64Value(11), trueLitteral, false},
		{"LT / Numbers / Greater", lt, int32Value(11), uint64Value(10), falseLitteral, false},
		{"LT / Numbers / Different sizes", lt, int64Value(math.MaxInt64), uint64Value(math.MaxUint64), trueLitteral, false},
		{"LT / Null", lt, nilLitteral.Value, nilLitteral.Value, falseLitteral, false},
		{"LT / Zero and Null", lt, stringValue(""), nilLitteral.Value, falseLitteral, false},
		{"LT / Null and Zero", lt, nilLitteral.Value, stringValue(""), falseLitteral, false},
		{"LT / Value and Null", lt, stringValue("foo"), nilLitteral.Value, falseLitteral, false},
		{"LT / Null and Value", lt, nilLitteral.Value, stringValue("foo"), falseLitteral, false},
		{"LT / String Bytes", lt, stringValue("foo1"), bytesValue([]byte("foo2")), trueLitteral, false},
		{"LT / Bytes String", lt, bytesValue([]byte("foo1")), stringValue("foo2"), trueLitteral, false},
		{"LTE / Same type", lte, int32Value(10), int32Value(11), trueLitteral, false},
		{"LTE / Same type / Greater", lte, int32Value(11), int32Value(10), falseLitteral, false},
		{"LTE / Numbers", lte, int32Value(10), uint64Value(11), trueLitteral, false},
		{"LTE / Numbers / Greater", lte, int32Value(11), uint64Value(10), falseLitteral, false},
		{"LTE / Null", lte, nilLitteral.Value, nilLitteral.Value, trueLitteral, false},
		{"LTE / Zero and Null", lte, stringValue(""), nilLitteral.Value, falseLitteral, false},
		{"LTE / Null and Zero", lte, nilLitteral.Value, stringValue(""), falseLitteral, false},
		{"LTE / Value and Null", lte, stringValue("foo"), nilLitteral.Value, falseLitteral, false},
		{"LTE / Null and Value", lte, nilLitteral.Value, stringValue("foo"), falseLitteral, false},
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
