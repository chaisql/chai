package document_test

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/genjidb/genji/document"
	"github.com/stretchr/testify/require"
)

var numericFuncs = []struct {
	name string
	fn   func(x interface{}) document.Value
}{
	{"integer", func(x interface{}) document.Value { return document.NewIntegerValue(int64(x.(int))) }},
	{"double", func(x interface{}) document.Value { return document.NewDoubleValue(float64(x.(int))) }},
	{"duration", func(x interface{}) document.Value { return document.NewDurationValue(time.Duration(int64(x.(int)))) }},
}

var textFuncs = []struct {
	name string
	fn   func(x interface{}) document.Value
}{
	{"text", func(x interface{}) document.Value { return document.NewTextValue(x.(string)) }},
	{"bytes", func(x interface{}) document.Value { return document.NewBlobValue([]byte(x.(string))) }},
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
		a  string
		b  string
	}{
		{"=", `{}`, `{}`},
		{"=", `{"a": 1}`, `{"a": 1}`},
		{
			"=",
			`{"a": 1, "b": 2}`,
			`{"b": 2, "a": 1}`,
		},
		{">", `{"a": 2}`, `{"a": 1}`},
		{"<", `{"a": 1}`, `{"a": 2}`},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%v %s %v", test.a, test.op, test.b), func(t *testing.T) {
			var ok bool
			var err error

			var d1, d2 document.FieldBuffer
			require.NoError(t, json.Unmarshal([]byte(test.a), &d1))
			require.NoError(t, json.Unmarshal([]byte(test.b), &d2))

			switch test.op {
			case "=":
				ok, err = document.NewDocumentValue(d1).IsEqual(document.NewDocumentValue(d2))
				require.NoError(t, err)
				require.True(t, ok)
			case ">":
				ok, err = document.NewDocumentValue(d1).IsGreaterThan(document.NewDocumentValue(d2))
				require.NoError(t, err)
				require.False(t, ok)
			case ">=":
				ok, err = document.NewDocumentValue(d1).IsGreaterThanOrEqual(document.NewDocumentValue(d2))
				require.NoError(t, err)
				require.False(t, ok)
			case "<":
				ok, err = document.NewDocumentValue(d1).IsLesserThan(document.NewDocumentValue(d2))
				require.NoError(t, err)
				require.False(t, ok)
			case "<=":
				ok, err = document.NewDocumentValue(d1).IsLesserThanOrEqual(document.NewDocumentValue(d2))
				require.NoError(t, err)
				require.False(t, ok)
			}
		})
	}
}

func TestComparisonArrays(t *testing.T) {
	tests := []struct {
		op       string
		a        string
		b        string
		expected bool
	}{
		{"=", `[]`, `[]`, true},
		{"=", `[1]`, `[1]`, true},
		{"=", `[1]`, `[]`, false},
		{"=", `[1.0, 2]`, `[1, 2]`, true},
		{"=", `[1,2,3]`, `[1,2,3]`, true},
		{"!=", `[1]`, `[5]`, true},
		{"!=", `[1]`, `[1, 1]`, true},
		{"!=", `[1,2,3]`, `[1,2,3]`, false},
		{"!=", `[1]`, `[]`, true},
		{">", `[2]`, `[1]`, true},
		{">", `[2]`, `[1, 1000]`, true},
		{">", `[1]`, `[1, 1000]`, false},
		{">", `[1, 2]`, `[1, 1000]`, false},
		{">", `[2, 1000]`, `[1]`, true},
		{">", `[2, 1000]`, `[2]`, true},
		{">", `[1,2,3]`, `[1,2,3]`, false},
		{">", `[1,2,3]`, `[]`, true},
		{">=", `[2]`, `[1]`, true},
		{">=", `[2]`, `[2]`, true},
		{">=", `[2]`, `[1, 1000]`, true},
		{">=", `[1]`, `[1, 1000]`, false},
		{">=", `[1, 2]`, `[1, 2]`, true},
		{">=", `[1, 2]`, `[1, 1000]`, false},
		{">=", `[2, 1000]`, `[1]`, true},
		{">=", `[2, 1000]`, `[2]`, true},
		{">=", `[1,2,3]`, `[1,2,3]`, true},
		{">=", `[1,2,3]`, `[]`, true},
		{"<", `[1]`, `[2]`, true},
		{"<", `[1,2,3]`, `[1,2]`, false},
		{"<", `[1,2,3]`, `[1,2,3]`, false},
		{"<", `[1,2]`, `[1,2,3]`, true},
		{"<", `[1, 1000]`, `[2]`, true},
		{"<", `[2]`, `[2, 1000]`, true},
		{"<", `[1,2,3]`, `[]`, false},
		{"<", `[]`, `[1,2,3]`, true},
		{"<=", `[1]`, `[2]`, true},
		{"<=", `[1, 1000]`, `[2]`, true},
		{"<=", `[1,2,3]`, `[1,2]`, false},
		{">=", `[2]`, `[1]`, true},
		{">=", `[2]`, `[2]`, true},
		{">=", `[2]`, `[1, 1000]`, true},
		{">=", `[2, 1000]`, `[1]`, true},
		{"<=", `[1,2,3]`, `[1,2,3]`, true},
		{"<=", `[]`, `[]`, true},
		{"<=", `[]`, `[1,2,3]`, true},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%v %s %v", test.a, test.op, test.b), func(t *testing.T) {
			var ok bool
			var err error

			var a1, a2 document.ValueBuffer
			require.NoError(t, json.Unmarshal([]byte(test.a), &a1))
			require.NoError(t, json.Unmarshal([]byte(test.b), &a2))

			switch test.op {
			case "=":
				ok, err = document.NewArrayValue(a1).IsEqual(document.NewArrayValue(a2))
			case ">":
				ok, err = document.NewArrayValue(a1).IsGreaterThan(document.NewArrayValue(a2))
			case ">=":
				ok, err = document.NewArrayValue(a1).IsGreaterThanOrEqual(document.NewArrayValue(a2))
			case "<":
				ok, err = document.NewArrayValue(a1).IsLesserThan(document.NewArrayValue(a2))
			case "<=":
				ok, err = document.NewArrayValue(a1).IsLesserThanOrEqual(document.NewArrayValue(a2))
			case "!=":
				ok, err = document.NewArrayValue(a1).IsNotEqual(document.NewArrayValue(a2))
			}
			require.NoError(t, err)
			require.Equal(t, test.expected, ok)
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
		ok, err := document.NewIntegerValue(1).IsNotEqual(document.NewTextValue("foo"))
		require.NoError(t, err)
		require.True(t, ok)
	})
}
