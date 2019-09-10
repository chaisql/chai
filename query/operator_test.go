package query

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOperator(t *testing.T) {
	tests := []struct {
		name      string
		fn        func(a, b Expr) Expr
		a, b, res Expr
	}{
		{"EQ / Same type", Eq, Int32Value(10), Int32Value(10), trueScalar},
		{"EQ / Same type / Different values", Eq, Int32Value(10), Int32Value(11), falseScalar},
		{"EQ / Numbers", Eq, Int32Value(10), Uint64Value(10), trueScalar},
		{"EQ / Numbers / Different values", Eq, Int32Value(10), Uint64Value(11), falseScalar},
		{"EQ / String Bytes", Eq, StringValue("foo"), BytesValue([]byte("foo")), trueScalar},
		{"EQ / Bytes String", Eq, BytesValue([]byte("foo")), StringValue("foo"), trueScalar},
		{"GT / Same type", Gt, Int32Value(11), Int32Value(10), trueScalar},
		{"GT / Same type / Lower", Gt, Int32Value(10), Int32Value(11), falseScalar},
		{"GT / Numbers", Gt, Int32Value(11), Uint64Value(10), trueScalar},
		{"GT / Numbers / Lower", Gt, Int32Value(10), Uint64Value(11), falseScalar},
		{"GT / String Bytes", Gt, StringValue("foo2"), BytesValue([]byte("foo1")), trueScalar},
		{"GT / Bytes String", Gt, BytesValue([]byte("foo2")), StringValue("foo1"), trueScalar},
		{"GTE / Same type", Gte, Int32Value(11), Int32Value(10), trueScalar},
		{"GTE / Same type / Lower", Gte, Int32Value(10), Int32Value(11), falseScalar},
		{"GTE / Numbers", Gte, Int32Value(11), Uint64Value(10), trueScalar},
		{"GTE / Numbers / Lower", Gte, Int32Value(10), Uint64Value(11), falseScalar},
		{"GTE / String Bytes", Gte, StringValue("foo2"), BytesValue([]byte("foo1")), trueScalar},
		{"GTE / Bytes String", Gte, BytesValue([]byte("foo2")), StringValue("foo1"), trueScalar},
		{"LT / Same type", Lt, Int32Value(10), Int32Value(11), trueScalar},
		{"LT / Same type / Greater", Lt, Int32Value(11), Int32Value(10), falseScalar},
		{"LT / Numbers", Lt, Int32Value(10), Uint64Value(11), trueScalar},
		{"LT / Numbers / Greater", Lt, Int32Value(11), Uint64Value(10), falseScalar},
		{"LT / String Bytes", Lt, StringValue("foo1"), BytesValue([]byte("foo2")), trueScalar},
		{"LT / Bytes String", Lt, BytesValue([]byte("foo1")), StringValue("foo2"), trueScalar},
		{"LTE / Same type", Lte, Int32Value(10), Int32Value(11), trueScalar},
		{"LTE / Same type / Greater", Lte, Int32Value(11), Int32Value(10), falseScalar},
		{"LTE / Numbers", Lte, Int32Value(10), Uint64Value(11), trueScalar},
		{"LTE / Numbers / Greater", Lte, Int32Value(11), Uint64Value(10), falseScalar},
		{"LTE / String Bytes", Lte, StringValue("foo1"), BytesValue([]byte("foo2")), trueScalar},
		{"LTE / Bytes String", Lte, BytesValue([]byte("foo1")), StringValue("foo2"), trueScalar},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			res, err := test.fn(test.a, test.b).Eval(EvalContext{})
			require.NoError(t, err)
			require.Equal(t, test.res, res)
		})
	}
}
