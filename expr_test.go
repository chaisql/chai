package genji

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOperator(t *testing.T) {
	tests := []struct {
		name string
		fn   func(a, b Expr) Expr
		a, b Expr
		res  Value
	}{
		{"EQ / Same type", Eq, Int32Value(10), Int32Value(10), trueLitteral},
		{"EQ / Same type / Different values", Eq, Int32Value(10), Int32Value(11), falseLitteral},
		{"EQ / Numbers", Eq, Int32Value(10), Uint64Value(10), trueLitteral},
		{"EQ / Numbers / Different values", Eq, Int32Value(10), Uint64Value(11), falseLitteral},
		{"EQ / String Bytes", Eq, StringValue("foo"), BytesValue([]byte("foo")), trueLitteral},
		{"EQ / Bytes String", Eq, BytesValue([]byte("foo")), StringValue("foo"), trueLitteral},
		{"GT / Same type", Gt, Int32Value(11), Int32Value(10), trueLitteral},
		{"GT / Same type / Lower", Gt, Int32Value(10), Int32Value(11), falseLitteral},
		{"GT / Numbers", Gt, Int32Value(11), Uint64Value(10), trueLitteral},
		{"GT / Numbers / Lower", Gt, Int32Value(10), Uint64Value(11), falseLitteral},
		{"GT / String Bytes", Gt, StringValue("foo2"), BytesValue([]byte("foo1")), trueLitteral},
		{"GT / Bytes String", Gt, BytesValue([]byte("foo2")), StringValue("foo1"), trueLitteral},
		{"GTE / Same type", Gte, Int32Value(11), Int32Value(10), trueLitteral},
		{"GTE / Same type / Lower", Gte, Int32Value(10), Int32Value(11), falseLitteral},
		{"GTE / Numbers", Gte, Int32Value(11), Uint64Value(10), trueLitteral},
		{"GTE / Numbers / Lower", Gte, Int32Value(10), Uint64Value(11), falseLitteral},
		{"GTE / String Bytes", Gte, StringValue("foo2"), BytesValue([]byte("foo1")), trueLitteral},
		{"GTE / Bytes String", Gte, BytesValue([]byte("foo2")), StringValue("foo1"), trueLitteral},
		{"LT / Same type", Lt, Int32Value(10), Int32Value(11), trueLitteral},
		{"LT / Same type / Greater", Lt, Int32Value(11), Int32Value(10), falseLitteral},
		{"LT / Numbers", Lt, Int32Value(10), Uint64Value(11), trueLitteral},
		{"LT / Numbers / Greater", Lt, Int32Value(11), Uint64Value(10), falseLitteral},
		{"LT / String Bytes", Lt, StringValue("foo1"), BytesValue([]byte("foo2")), trueLitteral},
		{"LT / Bytes String", Lt, BytesValue([]byte("foo1")), StringValue("foo2"), trueLitteral},
		{"LTE / Same type", Lte, Int32Value(10), Int32Value(11), trueLitteral},
		{"LTE / Same type / Greater", Lte, Int32Value(11), Int32Value(10), falseLitteral},
		{"LTE / Numbers", Lte, Int32Value(10), Uint64Value(11), trueLitteral},
		{"LTE / Numbers / Greater", Lte, Int32Value(11), Uint64Value(10), falseLitteral},
		{"LTE / String Bytes", Lte, StringValue("foo1"), BytesValue([]byte("foo2")), trueLitteral},
		{"LTE / Bytes String", Lte, BytesValue([]byte("foo1")), StringValue("foo2"), trueLitteral},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			res, err := test.fn(test.a, test.b).Eval(EvalStack{})
			require.NoError(t, err)
			require.Equal(t, test.res, res)
		})
	}
}
