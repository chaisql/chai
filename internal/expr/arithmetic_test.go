package expr_test

import (
	"math"
	"testing"

	"github.com/chaisql/chai/internal/expr"
	"github.com/chaisql/chai/internal/types"
	"github.com/stretchr/testify/require"
)

func TestValueAdd(t *testing.T) {
	tests := []struct {
		name           string
		v, u, expected types.Value
		fails          bool
	}{
		{"null+null", types.NewNullValue(), types.NewNullValue(), types.NewNullValue(), false},
		{"null+integer(10)", types.NewNullValue(), types.NewIntegerValue(10), types.NewNullValue(), false},
		{"bool(true)+bool(true)", types.NewBooleanValue(true), types.NewBooleanValue(true), types.NewNullValue(), false},
		{"bool(true)+bool(false)", types.NewBooleanValue(true), types.NewBooleanValue(true), types.NewNullValue(), false},
		{"bool(true)+integer(-10)", types.NewBooleanValue(true), types.NewIntegerValue(-10), types.NewNullValue(), false},
		{"integer(-10)+integer(10)", types.NewIntegerValue(-10), types.NewIntegerValue(10), types.NewIntegerValue(0), false},
		{"integer(120)+integer(120)", types.NewIntegerValue(120), types.NewIntegerValue(120), types.NewIntegerValue(240), false},
		{"integer(120)+float64(120)", types.NewIntegerValue(120), types.NewDoubleValue(120), types.NewDoubleValue(240), false},
		{"integer(120)+float64(120.1)", types.NewIntegerValue(120), types.NewDoubleValue(120.1), types.NewDoubleValue(240.1), false},
		{"int64(max)+integer(10)", types.NewBigintValue(math.MaxInt64), types.NewIntegerValue(10), nil, true},
		{"int64(min)+integer(-10)", types.NewBigintValue(math.MinInt64), types.NewIntegerValue(-10), nil, true},
		{"integer(120)+text('120')", types.NewIntegerValue(120), types.NewTextValue("120"), types.NewNullValue(), false},
		{"text('120')+text('120')", types.NewTextValue("120"), types.NewTextValue("120"), types.NewNullValue(), false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			v := expr.LiteralValue{Value: test.v}
			u := expr.LiteralValue{Value: test.u}
			res, err := expr.Add(v, u).Eval(nil)
			if test.fails {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, test.expected, res)
			}
		})
	}
}

func TestValueSub(t *testing.T) {
	tests := []struct {
		name           string
		v, u, expected types.Value
		fails          bool
	}{
		{"null-null", types.NewNullValue(), types.NewNullValue(), types.NewNullValue(), false},
		{"null-integer(10)", types.NewNullValue(), types.NewIntegerValue(10), types.NewNullValue(), false},
		{"bool(true)-bool(true)", types.NewBooleanValue(true), types.NewBooleanValue(true), types.NewNullValue(), false},
		{"bool(true)-bool(false)", types.NewBooleanValue(true), types.NewBooleanValue(false), types.NewNullValue(), false},
		{"bool(true)-integer(-10)", types.NewBooleanValue(true), types.NewIntegerValue(-10), types.NewNullValue(), false},
		{"integer(10)-integer(10)", types.NewIntegerValue(10), types.NewIntegerValue(10), types.NewIntegerValue(0), false},
		{"int16(250)-int16(220)", types.NewIntegerValue(250), types.NewIntegerValue(220), types.NewIntegerValue(30), false},
		{"integer(120)-float64(620)", types.NewIntegerValue(120), types.NewDoubleValue(620), types.NewDoubleValue(-500), false},
		{"integer(120)-float64(120.1)", types.NewIntegerValue(120), types.NewDoubleValue(120.1), types.NewDoubleValue(-0.09999999999999432), false},
		{"int64(min)-integer(10)", types.NewBigintValue(math.MinInt64), types.NewIntegerValue(10), nil, true},
		{"int64(max)-integer(-10)", types.NewBigintValue(math.MaxInt64), types.NewIntegerValue(-10), nil, true},
		{"integer(120)-text('120')", types.NewIntegerValue(120), types.NewTextValue("120"), types.NewNullValue(), false},
		{"text('120')-text('120')", types.NewTextValue("120"), types.NewTextValue("120"), types.NewNullValue(), false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			v := expr.LiteralValue{Value: test.v}
			u := expr.LiteralValue{Value: test.u}
			res, err := expr.Sub(v, u).Eval(nil)
			if test.fails {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, test.expected, res)
			}
		})
	}
}

func TestValueMult(t *testing.T) {
	tests := []struct {
		name           string
		v, u, expected types.Value
		fails          bool
	}{
		{"null*null", types.NewNullValue(), types.NewNullValue(), types.NewNullValue(), false},
		{"null*integer(10)", types.NewNullValue(), types.NewIntegerValue(10), types.NewNullValue(), false},
		{"bool(true)*bool(true)", types.NewBooleanValue(true), types.NewBooleanValue(true), types.NewNullValue(), false},
		{"bool(true)*bool(false)", types.NewBooleanValue(true), types.NewBooleanValue(false), types.NewNullValue(), false},
		{"bool(true)*integer(-10)", types.NewBooleanValue(true), types.NewIntegerValue(-10), types.NewNullValue(), false},
		{"integer(10)*integer(10)", types.NewIntegerValue(10), types.NewIntegerValue(10), types.NewIntegerValue(100), false},
		{"integer(10)*integer(80)", types.NewIntegerValue(10), types.NewIntegerValue(80), types.NewIntegerValue(800), false},
		{"integer(10)*float64(80)", types.NewIntegerValue(10), types.NewDoubleValue(80), types.NewDoubleValue(800), false},
		{"int64(max)*int64(max)", types.NewBigintValue(math.MaxInt64), types.NewBigintValue(math.MaxInt64), nil, true},
		{"integer(120)*text('120')", types.NewIntegerValue(120), types.NewTextValue("120"), types.NewNullValue(), false},
		{"text('120')*text('120')", types.NewTextValue("120"), types.NewTextValue("120"), types.NewNullValue(), false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			v := expr.LiteralValue{Value: test.v}
			u := expr.LiteralValue{Value: test.u}
			res, err := expr.Mul(v, u).Eval(nil)
			if test.fails {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, test.expected, res)
			}
		})
	}
}

func TestValueDiv(t *testing.T) {
	tests := []struct {
		name           string
		v, u, expected types.Value
		fails          bool
	}{
		{"null/null", types.NewNullValue(), types.NewNullValue(), types.NewNullValue(), false},
		{"null/integer(10)", types.NewNullValue(), types.NewIntegerValue(10), types.NewNullValue(), false},
		{"bool(true)/bool(true)", types.NewBooleanValue(true), types.NewBooleanValue(true), types.NewNullValue(), false},
		{"bool(true)/bool(false)", types.NewBooleanValue(true), types.NewBooleanValue(false), types.NewNullValue(), false},
		{"integer(10)/integer(0)", types.NewIntegerValue(10), types.NewIntegerValue(0), types.NewNullValue(), true},
		{"integer(10)/float64(0)", types.NewIntegerValue(10), types.NewDoubleValue(0), types.NewNullValue(), false},
		{"integer(10)/integer(10)", types.NewIntegerValue(10), types.NewIntegerValue(10), types.NewIntegerValue(1), false},
		{"integer(10)/integer(8)", types.NewIntegerValue(10), types.NewIntegerValue(8), types.NewIntegerValue(1), false},
		{"integer(10)/float64(8)", types.NewIntegerValue(10), types.NewDoubleValue(8), types.NewDoubleValue(1.25), false},
		{"int64(maxint)/float64(maxint)", types.NewBigintValue(math.MaxInt64), types.NewDoubleValue(math.MaxInt64), types.NewDoubleValue(1), false},
		{"integer(120)/text('120')", types.NewIntegerValue(120), types.NewTextValue("120"), types.NewNullValue(), false},
		{"text('120')/text('120')", types.NewTextValue("120"), types.NewTextValue("120"), types.NewNullValue(), false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			v := expr.LiteralValue{Value: test.v}
			u := expr.LiteralValue{Value: test.u}
			res, err := expr.Div(v, u).Eval(nil)
			if test.fails {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, test.expected, res)
			}
		})
	}
}

func TestValueMod(t *testing.T) {
	tests := []struct {
		name           string
		v, u, expected types.Value
		fails          bool
	}{
		{"null%null", types.NewNullValue(), types.NewNullValue(), types.NewNullValue(), false},
		{"null%integer(10)", types.NewNullValue(), types.NewIntegerValue(10), types.NewNullValue(), false},
		{"bool(true)%bool(true)", types.NewBooleanValue(true), types.NewBooleanValue(true), types.NewNullValue(), false},
		{"bool(true)%bool(false)", types.NewBooleanValue(true), types.NewBooleanValue(false), types.NewNullValue(), false},
		{"integer(10)%integer(0)", types.NewIntegerValue(10), types.NewIntegerValue(0), types.NewNullValue(), false},
		{"integer(10)%float64(0)", types.NewIntegerValue(10), types.NewDoubleValue(0), types.NewNullValue(), false},
		{"integer(10)%integer(10)", types.NewIntegerValue(10), types.NewIntegerValue(10), types.NewIntegerValue(0), false},
		{"integer(10)%integer(8)", types.NewIntegerValue(10), types.NewIntegerValue(8), types.NewIntegerValue(2), false},
		{"integer(10)%float64(8)", types.NewIntegerValue(10), types.NewDoubleValue(8), types.NewDoubleValue(2), false},
		{"int64(maxint)%float64(maxint)", types.NewBigintValue(math.MaxInt64), types.NewDoubleValue(math.MaxInt64), types.NewDoubleValue(0), false},
		{"double(> maxint)%int64(100)", types.NewDoubleValue(math.MaxInt64 + 1000), types.NewIntegerValue(100), types.NewDoubleValue(8), false},
		{"int64(100)%float64(> maxint)", types.NewIntegerValue(100), types.NewDoubleValue(math.MaxInt64 + 1000), types.NewDoubleValue(100), false},
		{"integer(120)%text('120')", types.NewIntegerValue(120), types.NewTextValue("120"), types.NewNullValue(), false},
		{"text('120')%text('120')", types.NewTextValue("120"), types.NewTextValue("120"), types.NewNullValue(), false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			v := expr.LiteralValue{Value: test.v}
			u := expr.LiteralValue{Value: test.u}
			res, err := expr.Mod(v, u).Eval(nil)
			if test.fails {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, test.expected, res)
			}
		})
	}
}

func TestValueBitwiseAnd(t *testing.T) {
	tests := []struct {
		name           string
		v, u, expected types.Value
		fails          bool
	}{
		{"null&null", types.NewNullValue(), types.NewNullValue(), types.NewNullValue(), false},
		{"null&integer(10)", types.NewNullValue(), types.NewIntegerValue(10), types.NewNullValue(), false},
		{"bool(true)&bool(true)", types.NewBooleanValue(true), types.NewBooleanValue(true), types.NewNullValue(), false},
		{"bool(true)&bool(false)", types.NewBooleanValue(true), types.NewBooleanValue(false), types.NewNullValue(), false},
		{"integer(10)&integer(0)", types.NewIntegerValue(10), types.NewIntegerValue(0), types.NewIntegerValue(0), false},
		{"double(10.5)&double(3.2)", types.NewDoubleValue(10.5), types.NewDoubleValue(3.2), types.NewNullValue(), false},
		{"integer(10)&double(0)", types.NewIntegerValue(10), types.NewDoubleValue(0), types.NewNullValue(), false},
		{"integer(10)&integer(10)", types.NewIntegerValue(10), types.NewIntegerValue(10), types.NewIntegerValue(10), false},
		{"integer(10)&integer(8)", types.NewIntegerValue(10), types.NewIntegerValue(8), types.NewIntegerValue(8), false},
		{"integer(10)&double(8)", types.NewIntegerValue(10), types.NewDoubleValue(8), types.NewNullValue(), false},
		{"text('120')&text('120')", types.NewTextValue("120"), types.NewTextValue("120"), types.NewNullValue(), false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			v := expr.LiteralValue{Value: test.v}
			u := expr.LiteralValue{Value: test.u}
			res, err := expr.BitwiseAnd(v, u).Eval(nil)
			if test.fails {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, test.expected, res)
			}
		})
	}
}

func TestValueBitwiseOr(t *testing.T) {
	tests := []struct {
		name           string
		v, u, expected types.Value
		fails          bool
	}{
		{"null|null", types.NewNullValue(), types.NewNullValue(), types.NewNullValue(), false},
		{"null|integer(10)", types.NewNullValue(), types.NewIntegerValue(10), types.NewNullValue(), false},
		{"bool(true)|bool(true)", types.NewBooleanValue(true), types.NewBooleanValue(true), types.NewNullValue(), false},
		{"bool(true)|bool(false)", types.NewBooleanValue(true), types.NewBooleanValue(false), types.NewNullValue(), false},
		{"integer(10)|integer(0)", types.NewIntegerValue(10), types.NewIntegerValue(0), types.NewIntegerValue(10), false},
		{"double(10.5)|double(3.2)", types.NewDoubleValue(10.5), types.NewDoubleValue(3.2), types.NewNullValue(), false},
		{"integer(10)|double(0)", types.NewIntegerValue(10), types.NewDoubleValue(0), types.NewNullValue(), false},
		{"integer(10)|integer(10)", types.NewIntegerValue(10), types.NewIntegerValue(10), types.NewIntegerValue(10), false},
		{"integer(10)|double(8)", types.NewIntegerValue(10), types.NewDoubleValue(8), types.NewNullValue(), false},
		{"text('120')|text('120')", types.NewTextValue("120"), types.NewTextValue("120"), types.NewNullValue(), false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			v := expr.LiteralValue{Value: test.v}
			u := expr.LiteralValue{Value: test.u}
			res, err := expr.BitwiseOr(v, u).Eval(nil)
			if test.fails {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, test.expected, res)
			}
		})
	}
}

func TestValueBitwiseXor(t *testing.T) {
	tests := []struct {
		name           string
		v, u, expected types.Value
		fails          bool
	}{
		{"null^null", types.NewNullValue(), types.NewNullValue(), types.NewNullValue(), false},
		{"null^integer(10)", types.NewNullValue(), types.NewIntegerValue(10), types.NewNullValue(), false},
		{"bool(true)^bool(true)", types.NewBooleanValue(true), types.NewBooleanValue(true), types.NewNullValue(), false},
		{"bool(true)^bool(false)", types.NewBooleanValue(true), types.NewBooleanValue(false), types.NewNullValue(), false},
		{"integer(10)^integer(0)", types.NewIntegerValue(10), types.NewIntegerValue(0), types.NewIntegerValue(10), false},
		{"double(10.5)^double(3.2)", types.NewDoubleValue(10.5), types.NewDoubleValue(3.2), types.NewNullValue(), false},
		{"integer(10)^double(0)", types.NewIntegerValue(10), types.NewDoubleValue(0), types.NewNullValue(), false},
		{"integer(10)^integer(10)", types.NewIntegerValue(10), types.NewIntegerValue(10), types.NewIntegerValue(0), false},
		{"text('120')^text('120')", types.NewTextValue("120"), types.NewTextValue("120"), types.NewNullValue(), false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			v := expr.LiteralValue{Value: test.v}
			u := expr.LiteralValue{Value: test.u}
			res, err := expr.BitwiseXor(v, u).Eval(nil)
			if test.fails {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, test.expected, res)
			}
		})
	}
}
