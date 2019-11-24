package query

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOperator(t *testing.T) {
	tests := []struct {
		name  string
		fn    func(a, b Expr) Expr
		a, b  Expr
		res   EvalValue
		fails bool
	}{
		{"EQ / Same type", Eq, Int32Value(10), Int32Value(10), trueLitteral, false},
		{"EQ / Same type / Different values", Eq, Int32Value(10), Int32Value(11), falseLitteral, false},
		{"EQ / Numbers", Eq, Int32Value(10), Float64Value(10), trueLitteral, false},
		{"EQ / Numbers / Different values", Eq, Int32Value(10), Uint64Value(11), falseLitteral, false},
		{"EQ / Numbers / List with one elem / Single value", Eq,
			LiteralExprList{Int32Value(10)}, Uint64Value(10), trueLitteral, false},
		{"EQ / Numbers / Single value / List with one elem", Eq,
			Uint64Value(10), LiteralExprList{Int32Value(10)}, trueLitteral, false},
		{"EQ / Lists", Eq,
			LiteralExprList{Uint64Value(10), StringValue("foo")}, LiteralExprList{Int32Value(10), BytesValue([]byte("foo"))}, trueLitteral, false},
		{"EQ / Lists / Different lengths", Eq,
			LiteralExprList{Uint64Value(10)}, LiteralExprList{Int32Value(10), BytesValue([]byte("foo"))}, falseLitteral, true},
		{"EQ / Null", Eq, nilLitteral.Value, nilLitteral.Value, trueLitteral, false},
		{"EQ / Zero and Null", Eq, IntValue(0), nilLitteral.Value, falseLitteral, false},
		{"EQ / String Bytes", Eq, StringValue("foo"), BytesValue([]byte("foo")), trueLitteral, false},
		{"EQ / Bytes String", Eq, BytesValue([]byte("foo")), StringValue("foo"), trueLitteral, false},
		{"GT / Same type", Gt, Int32Value(11), Int32Value(10), trueLitteral, false},
		{"GT / Same type / Lower", Gt, Int32Value(10), Int32Value(11), falseLitteral, false},
		{"GT / Numbers", Gt, Int32Value(11), Uint64Value(10), trueLitteral, false},
		{"GT / Numbers / Lower", Gt, Int32Value(10), Uint64Value(11), falseLitteral, false},
		{"GT / Null", Gt, nilLitteral.Value, nilLitteral.Value, falseLitteral, false},
		{"GT / Zero and Null", Gt, StringValue(""), nilLitteral.Value, falseLitteral, false},
		{"GT / Null and Zero", Gt, nilLitteral.Value, StringValue(""), falseLitteral, false},
		{"GT / Value and Null", Gt, StringValue("foo"), nilLitteral.Value, falseLitteral, false},
		{"GT / Null and Value", Gt, nilLitteral.Value, StringValue("foo"), falseLitteral, false},
		{"GT / String Bytes", Gt, StringValue("foo2"), BytesValue([]byte("foo1")), trueLitteral, false},
		{"GT / Bytes String", Gt, BytesValue([]byte("foo2")), StringValue("foo1"), trueLitteral, false},
		{"GT / Numbers / Different sizes", Gt, Uint64Value(math.MaxUint64), Int64Value(math.MaxInt64), trueLitteral, false},
		{"GTE / Same type", Gte, Int32Value(11), Int32Value(10), trueLitteral, false},
		{"GTE / Same type / Lower", Gte, Int32Value(10), Int32Value(11), falseLitteral, false},
		{"GTE / Numbers", Gte, Int32Value(11), Uint64Value(10), trueLitteral, false},
		{"GTE / Numbers / Lower", Gte, Int32Value(10), Uint64Value(11), falseLitteral, false},
		{"GTE / Null", Gte, nilLitteral.Value, nilLitteral.Value, trueLitteral, false},
		{"GTE / Zero and Null", Gte, StringValue(""), nilLitteral.Value, falseLitteral, false},
		{"GTE / Null and Zero", Gte, nilLitteral.Value, StringValue(""), falseLitteral, false},
		{"GTE / Value and Null", Gte, StringValue("foo"), nilLitteral.Value, falseLitteral, false},
		{"GTE / Null and Value", Gte, nilLitteral.Value, StringValue("foo"), falseLitteral, false},
		{"GTE / String Bytes", Gte, StringValue("foo2"), BytesValue([]byte("foo1")), trueLitteral, false},
		{"GTE / Bytes String", Gte, BytesValue([]byte("foo2")), StringValue("foo1"), trueLitteral, false},
		{"LT / Same type", Lt, Int32Value(10), Int32Value(11), trueLitteral, false},
		{"LT / Same type / Greater", Lt, Int32Value(11), Int32Value(10), falseLitteral, false},
		{"LT / Numbers", Lt, Int32Value(10), Uint64Value(11), trueLitteral, false},
		{"LT / Numbers / Greater", Lt, Int32Value(11), Uint64Value(10), falseLitteral, false},
		{"LT / Numbers / Different sizes", Lt, Int64Value(math.MaxInt64), Uint64Value(math.MaxUint64), trueLitteral, false},
		{"LT / Null", Lt, nilLitteral.Value, nilLitteral.Value, falseLitteral, false},
		{"LT / Zero and Null", Lt, StringValue(""), nilLitteral.Value, falseLitteral, false},
		{"LT / Null and Zero", Lt, nilLitteral.Value, StringValue(""), falseLitteral, false},
		{"LT / Value and Null", Lt, StringValue("foo"), nilLitteral.Value, falseLitteral, false},
		{"LT / Null and Value", Lt, nilLitteral.Value, StringValue("foo"), falseLitteral, false},
		{"LT / String Bytes", Lt, StringValue("foo1"), BytesValue([]byte("foo2")), trueLitteral, false},
		{"LT / Bytes String", Lt, BytesValue([]byte("foo1")), StringValue("foo2"), trueLitteral, false},
		{"LTE / Same type", Lte, Int32Value(10), Int32Value(11), trueLitteral, false},
		{"LTE / Same type / Greater", Lte, Int32Value(11), Int32Value(10), falseLitteral, false},
		{"LTE / Numbers", Lte, Int32Value(10), Uint64Value(11), trueLitteral, false},
		{"LTE / Numbers / Greater", Lte, Int32Value(11), Uint64Value(10), falseLitteral, false},
		{"LTE / Null", Lte, nilLitteral.Value, nilLitteral.Value, trueLitteral, false},
		{"LTE / Zero and Null", Lte, StringValue(""), nilLitteral.Value, falseLitteral, false},
		{"LTE / Null and Zero", Lte, nilLitteral.Value, StringValue(""), falseLitteral, false},
		{"LTE / Value and Null", Lte, StringValue("foo"), nilLitteral.Value, falseLitteral, false},
		{"LTE / Null and Value", Lte, nilLitteral.Value, StringValue("foo"), falseLitteral, false},
		{"LTE / String Bytes", Lte, StringValue("foo1"), BytesValue([]byte("foo2")), trueLitteral, false},
		{"LTE / Bytes String", Lte, BytesValue([]byte("foo1")), StringValue("foo2"), trueLitteral, false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			res, err := test.fn(test.a, test.b).Eval(EvalStack{})
			if test.fails {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, test.res, res)
		})
	}
}
