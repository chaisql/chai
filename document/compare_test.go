package document_test

import (
	"fmt"
	"math"
	"testing"

	"github.com/asdine/genji/document"
	"github.com/stretchr/testify/require"
)

var numericFuncs = []struct {
	name string
	fn   func(x interface{}) document.Value
}{
	{"uint8", func(x interface{}) document.Value { return document.NewUint8Value(uint8(x.(int))) }},
	{"uint16", func(x interface{}) document.Value { return document.NewUint16Value(uint16(x.(int))) }},
	{"uint32", func(x interface{}) document.Value { return document.NewUint32Value(uint32(x.(int))) }},
	{"uint64", func(x interface{}) document.Value { return document.NewUint64Value(uint64(x.(int))) }},
	{"int8", func(x interface{}) document.Value { return document.NewInt8Value(int8(x.(int))) }},
	{"int16", func(x interface{}) document.Value { return document.NewInt16Value(int16(x.(int))) }},
	{"int32", func(x interface{}) document.Value { return document.NewInt32Value(int32(x.(int))) }},
	{"int64", func(x interface{}) document.Value { return document.NewInt64Value(int64(x.(int))) }},
	{"float64", func(x interface{}) document.Value { return document.NewFloat64Value(float64(x.(int))) }},
}

var textFuncs = []struct {
	name string
	fn   func(x interface{}) document.Value
}{
	{"string", func(x interface{}) document.Value { return document.NewStringValue(x.(string)) }},
	{"bytes", func(x interface{}) document.Value { return document.NewBytesValue([]byte(x.(string))) }},
}

func TestComparisonNumbers(t *testing.T) {
	tests := []struct {
		op   string
		a, b int
		ok   bool
	}{
		{"=", 2, 1, false},
		{"=", 2, 2, true},
		{"!=", 2, 1, true},
		{"!=", 2, 2, false},
		{">", 2, 1, true},
		{">", 1, 2, false},
		{">", 2, 2, false},
		{">=", 2, 1, true},
		{">=", 1, 2, false},
		{">=", 2, 2, true},
		{"<", 2, 1, false},
		{"<", 1, 2, true},
		{"<", 2, 2, false},
		{"<=", 2, 1, false},
		{"<=", 1, 2, true},
		{"<=", 2, 2, true},
	}

	for i := 0; i < len(numericFuncs); i++ {
		for j := 0; j < len(numericFuncs); j++ {
			for _, test := range tests {
				t.Run(fmt.Sprintf("%s(%d)%s%s(%d)", numericFuncs[i].name, test.a, test.op, numericFuncs[j].name, test.b), func(t *testing.T) {
					var ok bool
					var err error

					switch test.op {
					case "=":
						ok, err = numericFuncs[i].fn(test.a).IsEqual(numericFuncs[j].fn(test.b))
					case "!=":
						ok, err = numericFuncs[i].fn(test.a).IsNotEqual(numericFuncs[j].fn(test.b))
					case ">":
						ok, err = numericFuncs[i].fn(test.a).IsGreaterThan(numericFuncs[j].fn(test.b))
					case ">=":
						ok, err = numericFuncs[i].fn(test.a).IsGreaterThanOrEqual(numericFuncs[j].fn(test.b))
					case "<":
						ok, err = numericFuncs[i].fn(test.a).IsLesserThan(numericFuncs[j].fn(test.b))
					case "<=":
						ok, err = numericFuncs[i].fn(test.a).IsLesserThanOrEqual(numericFuncs[j].fn(test.b))
					}
					require.NoError(t, err)
					require.Equal(t, test.ok, ok)
				})
			}
		}
	}

	t.Run("uint64", func(t *testing.T) {
		a := document.NewUint64Value(math.MaxUint64)
		b := document.NewInt64Value(10)

		ok, err := a.IsEqual(b)
		require.NoError(t, err)
		require.False(t, ok)

		ok, err = a.IsEqual(a)
		require.NoError(t, err)
		require.True(t, ok)

		ok, err = a.IsNotEqual(b)
		require.NoError(t, err)
		require.True(t, ok)

		ok, err = a.IsNotEqual(a)
		require.NoError(t, err)
		require.False(t, ok)

		ok, err = a.IsGreaterThan(b)
		require.NoError(t, err)
		require.True(t, ok)

		ok, err = b.IsGreaterThan(a)
		require.NoError(t, err)
		require.False(t, ok)

		ok, err = a.IsLesserThanOrEqual(b)
		require.NoError(t, err)
		require.False(t, ok)

		ok, err = b.IsLesserThanOrEqual(a)
		require.NoError(t, err)
		require.True(t, ok)
	})
}

func TestComparisonNumbersWithNull(t *testing.T) {
	tests := []struct {
		op string
		a  int
		ok bool
	}{
		{"=", 1, false},
		{"=", 0, false},
		{"!=", 0, true},
		{"!=", 1, true},
		{">", 1, false},
		{">", 0, false},
		{">=", 1, false},
		{">=", 0, false},
		{"<", 1, false},
		{"<", 0, false},
		{"<=", 1, false},
		{"<=", 0, false},
	}

	for i := 0; i < len(numericFuncs); i++ {
		for j := 0; j < len(numericFuncs); j++ {
			for _, test := range tests {
				t.Run(fmt.Sprintf("%s(%q)%sNULL", numericFuncs[i].name, test.a, test.op), func(t *testing.T) {
					var ok bool
					var err error

					switch test.op {
					case "=":
						ok, err = numericFuncs[i].fn(test.a).IsEqual(document.NewNullValue())
					case "!=":
						ok, err = numericFuncs[i].fn(test.a).IsNotEqual(document.NewNullValue())
					case ">":
						ok, err = numericFuncs[i].fn(test.a).IsGreaterThan(document.NewNullValue())
					case ">=":
						ok, err = numericFuncs[i].fn(test.a).IsGreaterThanOrEqual(document.NewNullValue())
					case "<":
						ok, err = numericFuncs[i].fn(test.a).IsLesserThan(document.NewNullValue())
					case "<=":
						ok, err = numericFuncs[i].fn(test.a).IsLesserThanOrEqual(document.NewNullValue())
					}
					require.NoError(t, err)
					require.Equal(t, test.ok, ok)
				})
			}
		}
	}
}

func TestComparisonText(t *testing.T) {
	tests := []struct {
		op   string
		a, b string
		ok   bool
	}{
		{"=", "b", "a", false},
		{"=", "b", "b", true},
		{"!=", "b", "a", true},
		{"!=", "b", "b", false},
		{">", "b", "a", true},
		{">", "a", "b", false},
		{">", "b", "b", false},
		{">=", "b", "a", true},
		{">=", "a", "b", false},
		{">=", "b", "b", true},
		{"<", "b", "a", false},
		{"<", "a", "b", true},
		{"<", "b", "b", false},
		{"<=", "b", "a", false},
		{"<=", "a", "b", true},
		{"<=", "b", "b", true},
	}

	for i := 0; i < len(textFuncs); i++ {
		for j := 0; j < len(textFuncs); j++ {
			for _, test := range tests {
				t.Run(fmt.Sprintf("%s(%q)%s%s(%q)", textFuncs[i].name, test.a, test.op, textFuncs[j].name, test.b), func(t *testing.T) {
					var ok bool
					var err error

					switch test.op {
					case "=":
						ok, err = textFuncs[i].fn(test.a).IsEqual(textFuncs[j].fn(test.b))
					case "!=":
						ok, err = textFuncs[i].fn(test.a).IsNotEqual(textFuncs[j].fn(test.b))
					case ">":
						ok, err = textFuncs[i].fn(test.a).IsGreaterThan(textFuncs[j].fn(test.b))
					case ">=":
						ok, err = textFuncs[i].fn(test.a).IsGreaterThanOrEqual(textFuncs[j].fn(test.b))
					case "<":
						ok, err = textFuncs[i].fn(test.a).IsLesserThan(textFuncs[j].fn(test.b))
					case "<=":
						ok, err = textFuncs[i].fn(test.a).IsLesserThanOrEqual(textFuncs[j].fn(test.b))
					}
					require.NoError(t, err)
					require.Equal(t, test.ok, ok)
				})
			}
		}
	}

}

func TestComparisonTextWithNull(t *testing.T) {
	nullTextTests := []struct {
		op string
		a  string
		ok bool
	}{
		{"=", "a", false},
		{"=", "", false},
		{"!=", "a", true},
		{"!=", "", true},
		{">", "a", false},
		{">", "", false},
		{">=", "a", false},
		{">=", "", false},
		{"<", "a", false},
		{"<", "", false},
		{"<=", "a", false},
		{"<=", "", false},
	}

	for i := 0; i < len(textFuncs); i++ {
		for j := 0; j < len(textFuncs); j++ {
			for _, test := range nullTextTests {
				t.Run(fmt.Sprintf("%s(%q)%sNULL", textFuncs[i].name, test.a, test.op), func(t *testing.T) {
					var ok bool
					var err error

					switch test.op {
					case "=":
						ok, err = textFuncs[i].fn(test.a).IsEqual(document.NewNullValue())
					case "!=":
						ok, err = textFuncs[i].fn(test.a).IsNotEqual(document.NewNullValue())
					case ">":
						ok, err = textFuncs[i].fn(test.a).IsGreaterThan(document.NewNullValue())
					case ">=":
						ok, err = textFuncs[i].fn(test.a).IsGreaterThanOrEqual(document.NewNullValue())
					case "<":
						ok, err = textFuncs[i].fn(test.a).IsLesserThan(document.NewNullValue())
					case "<=":
						ok, err = textFuncs[i].fn(test.a).IsLesserThanOrEqual(document.NewNullValue())
					}
					require.NoError(t, err)
					require.Equal(t, test.ok, ok)
				})
			}
		}
	}
}

func TestComparisonDocuments(t *testing.T) {
	tests := []struct {
		op string
		a  document.Document
		b  document.Document
	}{
		{"=", document.NewFieldBuffer(), document.NewFieldBuffer()},
		{"=", document.NewFieldBuffer().Add("a", document.NewIntValue(1)), document.NewFieldBuffer().Add("a", document.NewIntValue(1))},
		{
			"=",
			document.NewFieldBuffer().
				Add("a", document.NewInt32Value(1)).
				Add("b", document.NewUint64Value(2)),
			document.NewFieldBuffer().
				Add("b", document.NewFloat64Value(2)).
				Add("a", document.NewInt8Value(1)),
		},
		{">", document.NewFieldBuffer().Add("a", document.NewInt8Value(2)), document.NewFieldBuffer().Add("a", document.NewInt64Value(1))},
		{"<", document.NewFieldBuffer().Add("a", document.NewFloat64Value(1)), document.NewFieldBuffer().Add("a", document.NewInt8Value(2))},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%v %s %v", test.a, test.op, test.b), func(t *testing.T) {
			var ok bool
			var err error

			switch test.op {
			case "=":
				ok, err = document.NewDocumentValue(test.a).IsEqual(document.NewDocumentValue(test.b))
				require.NoError(t, err)
				require.True(t, ok)
			case ">":
				ok, err = document.NewDocumentValue(test.a).IsGreaterThan(document.NewDocumentValue(test.b))
				require.Error(t, err)
			case ">=":
				ok, err = document.NewDocumentValue(test.a).IsGreaterThanOrEqual(document.NewDocumentValue(test.b))
				require.Error(t, err)
			case "<":
				ok, err = document.NewDocumentValue(test.a).IsLesserThan(document.NewDocumentValue(test.b))
				require.Error(t, err)
			case "<=":
				ok, err = document.NewDocumentValue(test.a).IsLesserThanOrEqual(document.NewDocumentValue(test.b))
				require.Error(t, err)
			}
		})
	}
}

func TestComparisonArrays(t *testing.T) {
	tests := []struct {
		op string
		a  document.Array
		b  document.Array
	}{
		{"=", document.NewValueBuffer(), document.NewValueBuffer()},
		{"=", document.NewValueBuffer().Append(document.NewInt64Value(1)), document.NewValueBuffer().Append(document.NewInt8Value(1))},
		{
			"=",
			document.NewValueBuffer().
				Append(document.NewInt64Value(1)).
				Append(document.NewIntValue(2)),
			document.NewValueBuffer().
				Append(document.NewFloat64Value(1)).
				Append(document.NewIntValue(2)),
		},
		{"!=", document.NewValueBuffer().Append(document.NewInt64Value(1)), document.NewValueBuffer().Append(document.NewInt8Value(5))},
		{"!=", document.NewValueBuffer().Append(document.NewInt64Value(1)), document.NewValueBuffer().Append(document.NewInt8Value(1)).Append(document.NewInt8Value(1))},
		{">", document.NewValueBuffer().Append(document.NewIntValue(2)), document.NewValueBuffer().Append(document.NewIntValue(1))},
		{">",
			document.NewValueBuffer().Append(document.NewIntValue(2)),
			document.NewValueBuffer().Append(document.NewIntValue(1)).Append(document.NewIntValue(1000))},
		{">",
			document.NewValueBuffer().Append(document.NewIntValue(2)).Append(document.NewIntValue(1000)),
			document.NewValueBuffer().Append(document.NewIntValue(1))},
		{">",
			document.NewValueBuffer().Append(document.NewIntValue(2)).Append(document.NewIntValue(1000)),
			document.NewValueBuffer().Append(document.NewIntValue(2))},
		{"<", document.NewValueBuffer().Append(document.NewIntValue(1)), document.NewValueBuffer().Append(document.NewIntValue(2))},
		{"<",
			document.NewValueBuffer().Append(document.NewIntValue(1)).Append(document.NewIntValue(1000)),
			document.NewValueBuffer().Append(document.NewIntValue(2))},
		{"<",
			document.NewValueBuffer().Append(document.NewIntValue(2)),
			document.NewValueBuffer().Append(document.NewIntValue(2)).Append(document.NewIntValue(1000))},
		{"<=", document.NewValueBuffer().Append(document.NewIntValue(1)), document.NewValueBuffer().Append(document.NewIntValue(2))},
		{"<=",
			document.NewValueBuffer().Append(document.NewIntValue(1)).Append(document.NewIntValue(1000)),
			document.NewValueBuffer().Append(document.NewIntValue(2))},
		{">=", document.NewValueBuffer().Append(document.NewIntValue(2)), document.NewValueBuffer().Append(document.NewIntValue(1))},
		{">=", document.NewValueBuffer().Append(document.NewIntValue(2)), document.NewValueBuffer().Append(document.NewIntValue(2))},
		{">=",
			document.NewValueBuffer().Append(document.NewIntValue(2)),
			document.NewValueBuffer().Append(document.NewIntValue(1)).Append(document.NewIntValue(1000))},
		{">=",
			document.NewValueBuffer().Append(document.NewIntValue(2)).Append(document.NewIntValue(1000)),
			document.NewValueBuffer().Append(document.NewIntValue(1))},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%v %s %v", test.a, test.op, test.b), func(t *testing.T) {
			var ok bool
			var err error

			switch test.op {
			case "=":
				ok, err = document.NewArrayValue(test.a).IsEqual(document.NewArrayValue(test.b))
			case ">":
				ok, err = document.NewArrayValue(test.a).IsGreaterThan(document.NewArrayValue(test.b))
			case ">=":
				ok, err = document.NewArrayValue(test.a).IsGreaterThanOrEqual(document.NewArrayValue(test.b))
			case "<":
				ok, err = document.NewArrayValue(test.a).IsLesserThan(document.NewArrayValue(test.b))
			case "<=":
				ok, err = document.NewArrayValue(test.a).IsLesserThanOrEqual(document.NewArrayValue(test.b))
			case "!=":
				ok, err = document.NewArrayValue(test.a).IsNotEqual(document.NewArrayValue(test.b))
			}
			require.NoError(t, err)
			require.True(t, ok)
		})
	}
}

func TestComparisonDifferentTypes(t *testing.T) {
	tests := []struct {
		op string
		a  int
		b  string
	}{
		{"=", 1, "1"},
		{"=", 0, ""},
		{"=", 0, ""},
		{">", 2, "1"},
		{">", 1, ""},
		{">=", 1, "1"},
		{">=", 0, ""},
		{"<", 0, "1"},
		{"<", -1, ""},
		{"<=", 1, "1"},
		{"<=", 0, ""},
	}

	for i := 0; i < len(numericFuncs); i++ {
		for j := 0; j < len(textFuncs); j++ {
			for _, test := range tests {
				t.Run(fmt.Sprintf("%s(%d)%s%s(%q)", numericFuncs[i].name, test.a, test.op, textFuncs[j].name, test.b), func(t *testing.T) {
					var ok bool
					var err error

					switch test.op {
					case "=":
						ok, err = numericFuncs[i].fn(test.a).IsEqual(textFuncs[j].fn(test.b))
					case ">":
						ok, err = numericFuncs[i].fn(test.a).IsGreaterThan(textFuncs[j].fn(test.b))
					case ">=":
						ok, err = numericFuncs[i].fn(test.a).IsGreaterThanOrEqual(textFuncs[j].fn(test.b))
					case "<":
						ok, err = numericFuncs[i].fn(test.a).IsLesserThan(textFuncs[j].fn(test.b))
					case "<=":
						ok, err = numericFuncs[i].fn(test.a).IsLesserThanOrEqual(textFuncs[j].fn(test.b))
					}
					require.NoError(t, err)
					require.False(t, ok)
				})
			}
		}
	}

	t.Run("not equal with different types", func(t *testing.T) {
		ok, err := document.NewIntValue(1).IsNotEqual(document.NewStringValue("foo"))
		require.NoError(t, err)
		require.True(t, ok)
	})
}
