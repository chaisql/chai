package document_test

import (
	"fmt"
	"math"
	"testing"

	"github.com/asdine/genji/document"
	"github.com/stretchr/testify/require"
)


func TestComparison(t *testing.T) {
	numericFuncs := []struct {
		name string
		fn   func(x interface{}) document.Value
	}{
		{"uint8", func(x interface{}) document.Value { return document.NewUint8(uint8(x.(int))) }},
		{"uint16", func(x interface{}) document.Value { return document.NewUint16(uint16(x.(int))) }},
		{"uint32", func(x interface{}) document.Value { return document.NewUint32(uint32(x.(int))) }},
		{"uint64", func(x interface{}) document.Value { return document.NewUint64(uint64(x.(int))) }},
		{"int8", func(x interface{}) document.Value { return document.NewInt8(int8(x.(int))) }},
		{"int16", func(x interface{}) document.Value { return document.NewInt16(int16(x.(int))) }},
		{"int32", func(x interface{}) document.Value { return document.NewInt32(int32(x.(int))) }},
		{"int64", func(x interface{}) document.Value { return document.NewInt64(int64(x.(int))) }},
		{"float64", func(x interface{}) document.Value { return document.NewFloat64(float64(x.(int))) }},
	}

	numericTests := []struct {
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

	nullNumericTests := []struct {
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
			for _, test := range numericTests {
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

		for _, test := range nullNumericTests {
			t.Run(fmt.Sprintf("%s(%q)%sNULL", numericFuncs[i].name, test.a, test.op), func(t *testing.T) {
				var ok bool
				var err error

				switch test.op {
				case "=":
					ok, err = numericFuncs[i].fn(test.a).IsEqual(document.NewNull())
				case "!=":
					ok, err = numericFuncs[i].fn(test.a).IsNotEqual(document.NewNull())
				case ">":
					ok, err = numericFuncs[i].fn(test.a).IsGreaterThan(document.NewNull())
				case ">=":
					ok, err = numericFuncs[i].fn(test.a).IsGreaterThanOrEqual(document.NewNull())
				case "<":
					ok, err = numericFuncs[i].fn(test.a).IsLesserThan(document.NewNull())
				case "<=":
					ok, err = numericFuncs[i].fn(test.a).IsLesserThanOrEqual(document.NewNull())
				}
				require.NoError(t, err)
				require.Equal(t, test.ok, ok)
			})
		}
	}

	textFuncs := []struct {
		name string
		fn   func(x interface{}) document.Value
	}{
		{"string", func(x interface{}) document.Value { return document.NewString(x.(string)) }},
		{"bytes", func(x interface{}) document.Value { return document.NewBytes([]byte(x.(string))) }},
	}

	textTests := []struct {
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
			for _, test := range textTests {
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

			for _, test := range nullTextTests {
				t.Run(fmt.Sprintf("%s(%q)%sNULL", textFuncs[i].name, test.a, test.op), func(t *testing.T) {
					var ok bool
					var err error

					switch test.op {
					case "=":
						ok, err = textFuncs[i].fn(test.a).IsEqual(document.NewNull())
					case "!=":
						ok, err = textFuncs[i].fn(test.a).IsNotEqual(document.NewNull())
					case ">":
						ok, err = textFuncs[i].fn(test.a).IsGreaterThan(document.NewNull())
					case ">=":
						ok, err = textFuncs[i].fn(test.a).IsGreaterThanOrEqual(document.NewNull())
					case "<":
						ok, err = textFuncs[i].fn(test.a).IsLesserThan(document.NewNull())
					case "<=":
						ok, err = textFuncs[i].fn(test.a).IsLesserThanOrEqual(document.NewNull())
					}
					require.NoError(t, err)
					require.Equal(t, test.ok, ok)
				})
			}
		}
	}

	incompatibleTests := []struct {
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
			for _, test := range incompatibleTests {
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

	t.Run("uint64", func(t *testing.T) {
		a := document.NewUint64(math.MaxUint64)
		b := document.NewInt64(10)

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

	t.Run("not equal with different types", func(t *testing.T) {
		ok, err := document.NewInt(1).IsNotEqual(document.NewString("foo"))
		require.NoError(t, err)
		require.True(t, ok)
	})
}
