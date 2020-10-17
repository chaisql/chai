package document_test

import (
	"testing"

	"github.com/genjidb/genji/document"
	"github.com/stretchr/testify/require"
)

func TestNewFromJSON(t *testing.T) {
	tests := []struct {
		name     string
		data     string
		expected *document.FieldBuffer
		fails    bool
	}{
		{"empty object", "{}", document.NewFieldBuffer(), false},
		{"empty object, missing closing bracket", "{", nil, true},
		{"classic object", `{"a": 1, "b": true, "c": "hello", "d": [1, 2, 3], "e": {"f": "g"}}`,
			document.NewFieldBuffer().
				Add("a", document.NewIntegerValue(1)).
				Add("b", document.NewBoolValue(true)).
				Add("c", document.NewTextValue("hello")).
				Add("d", document.NewArrayValue(document.NewValueBuffer().
					Append(document.NewIntegerValue(1)).
					Append(document.NewIntegerValue(2)).
					Append(document.NewIntegerValue(3)))).
				Add("e", document.NewDocumentValue(document.NewFieldBuffer().Add("f", document.NewTextValue("g")))),
			false},
		{"string values", `{"a": "hello ciao"}`, document.NewFieldBuffer().Add("a", document.NewTextValue("hello ciao")), false},
		{"+integer values", `{"a": 1000}`, document.NewFieldBuffer().Add("a", document.NewIntegerValue(1000)), false},
		{"-integer values", `{"a": -1000}`, document.NewFieldBuffer().Add("a", document.NewIntegerValue(-1000)), false},
		{"+float values", `{"a": 10000000000.0}`, document.NewFieldBuffer().Add("a", document.NewDoubleValue(10000000000)), false},
		{"-float values", `{"a": -10000000000.0}`, document.NewFieldBuffer().Add("a", document.NewDoubleValue(-10000000000)), false},
		{"bool values", `{"a": true, "b": false}`, document.NewFieldBuffer().Add("a", document.NewBoolValue(true)).Add("b", document.NewBoolValue(false)), false},
		{"empty arrays", `{"a": []}`, document.NewFieldBuffer().Add("a", document.NewArrayValue(document.NewValueBuffer())), false},
		{"nested arrays", `{"a": [[1,  2]]}`, document.NewFieldBuffer().
			Add("a", document.NewArrayValue(
				document.NewValueBuffer().
					Append(document.NewArrayValue(
						document.NewValueBuffer().
							Append(document.NewIntegerValue(1)).
							Append(document.NewIntegerValue(2)))))), false},
		{"missing comma", `{"a": 1 "b": 2}`, nil, true},
		{"missing closing brackets", `{"a": 1, "b": 2`, nil, true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			d := document.NewFromJSON([]byte(test.data))

			fb := document.NewFieldBuffer()
			err := fb.Copy(d)

			if test.fails {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, *test.expected, *fb)
			}
		})
	}
}
