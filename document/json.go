// +build !wasm

package document

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"
)

// MarshalJSON implements the json.Marshaler interface.
func (v Value) MarshalJSON() ([]byte, error) {
	var x interface{}

	switch v.Type {
	case DocumentValue:
		d, err := v.ConvertToDocument()
		if err != nil {
			return nil, err
		}
		x = &jsonDocument{d}
	case ArrayValue:
		a, err := v.ConvertToArray()
		if err != nil {
			return nil, err
		}
		x = &jsonArray{a}
	case TextValue, BlobValue:
		s, err := v.ConvertToText()
		if err != nil {
			return nil, err
		}
		x = s
	default:
		x = v.V
	}

	return json.Marshal(x)
}

// String returns a string representation of the value. It implements the fmt.Stringer interface.
func (v Value) String() string {
	switch v.Type {
	case DocumentValue:
		var buf bytes.Buffer
		err := ToJSON(&buf, v.V.(Document))
		if err != nil {
			panic(err)
		}
		return buf.String()
	case ArrayValue:
		var buf bytes.Buffer
		err := ArrayToJSON(&buf, v.V.(Array))
		if err != nil {
			panic(err)
		}
		return buf.String()
	case NullValue:
		return "NULL"
	case TextValue:
		return string(v.V.([]byte))
	}

	return fmt.Sprintf("%v", v.V)
}

// MarshalJSON implements the json.Marshaler interface.
func (fb *FieldBuffer) MarshalJSON() ([]byte, error) {
	return json.Marshal(jsonDocument{Document: fb})
}

// UnmarshalJSON implements the json.Unmarshaler interface.
func (fb *FieldBuffer) UnmarshalJSON(data []byte) error {
	dec := json.NewDecoder(bytes.NewReader(data))

	t, err := dec.Token()
	if err == io.EOF {
		return err
	}

	return parseJSONDocument(dec, t, fb)
}

// UnmarshalJSON implements the json.Unmarshaler interface.
func (vb *ValueBuffer) UnmarshalJSON(data []byte) error {
	dec := json.NewDecoder(bytes.NewReader(data))

	t, err := dec.Token()
	if err == io.EOF {
		return err
	}

	return parseJSONArray(dec, t, vb)
}

// ToJSON encodes d to w in JSON.
func ToJSON(w io.Writer, d Document) error {
	return json.NewEncoder(w).Encode(jsonDocument{d})
}

// ArrayToJSON encodes a to w in JSON.
func ArrayToJSON(w io.Writer, a Array) error {
	return json.NewEncoder(w).Encode(jsonArray{a})
}

type jsonArray struct {
	Array
}

func (j jsonArray) MarshalJSON() ([]byte, error) {
	var buf bytes.Buffer

	buf.WriteByte('[')
	var notFirst bool
	err := j.Array.Iterate(func(i int, v Value) error {
		if notFirst {
			buf.WriteByte(',')
		}
		notFirst = true

		data, err := v.MarshalJSON()
		if err != nil {
			return err
		}

		_, err = buf.Write(data)
		return err
	})
	if err != nil {
		return nil, err
	}
	buf.WriteByte(']')
	return buf.Bytes(), nil
}

type jsonDocument struct {
	Document
}

func (j jsonDocument) MarshalJSON() ([]byte, error) {
	var buf bytes.Buffer

	buf.WriteByte('{')

	var notFirst bool
	err := j.Document.Iterate(func(f string, v Value) error {
		if notFirst {
			buf.WriteByte(',')
		}
		notFirst = true

		buf.WriteString(strconv.Quote(f))
		buf.WriteRune(':')

		data, err := v.MarshalJSON()
		if err != nil {
			return err
		}
		_, err = buf.Write(data)
		return err
	})
	if err != nil {
		return nil, err
	}

	buf.WriteByte('}')

	return buf.Bytes(), nil
}

func parseJSONValue(dec *json.Decoder) (Value, error) {
	// ensure the decoder parses numbers as the json.Number type
	dec.UseNumber()

	// parse the first token to determine which type is it
	t, err := dec.Token()
	if err != nil {
		return Value{}, err
	}

	switch tt := t.(type) {
	case string:
		return NewTextValue(tt), nil
	case bool:
		return NewBoolValue(tt), nil
	case nil:
		return NewNullValue(), nil
	case json.Number:
		i, err := tt.Int64()
		if err != nil {
			// if it's too big to fit in an int64, let's try parsing this as a floating point number
			f, err := tt.Float64()
			if err != nil {
				return Value{}, err
			}

			return NewFloat64Value(f), nil
		}

		switch {
		case i >= math.MinInt8 && i <= math.MaxInt8:
			return NewInt8Value(int8(i)), nil
		case i >= math.MinInt16 && i <= math.MaxInt16:
			return NewInt16Value(int16(i)), nil
		case i >= math.MinInt32 && i <= math.MaxInt32:
			return NewInt32Value(int32(i)), nil
		default:
			return NewInt64Value(int64(i)), nil
		}
	case json.Delim:
		switch tt {
		case ']', '}':
			return Value{}, fmt.Errorf("found %q, expected '{' or '['", tt)
		case '[':
			buf := NewValueBuffer()
			err := parseJSONArray(dec, t, &buf)
			if err != nil {
				return Value{}, err
			}

			return NewArrayValue(buf), nil
		case '{':
			buf := NewFieldBuffer()
			err = parseJSONDocument(dec, t, buf)
			if err != nil {
				return Value{}, err
			}

			return NewDocumentValue(buf), nil
		}
	}

	return Value{}, nil
}

func parseJSONDocument(dec *json.Decoder, currToken json.Token, buf *FieldBuffer) error {
	var err error

	// expecting a '{'
	if d, ok := currToken.(json.Delim); !ok || d.String() != "{" {
		return fmt.Errorf("found %q, expected '{'", d.String())
	}

	for dec.More() {
		// parse the key, it must be a string
		t, err := dec.Token()
		if err != nil {
			return err
		}

		k, ok := t.(string)
		if !ok {
			return fmt.Errorf("found %q, expected string", t)
		}

		v, err := parseJSONValue(dec)
		if err != nil {
			return err
		}

		buf.Add(k, v)
	}

	currToken, err = dec.Token()
	if err == io.EOF {
		return fmt.Errorf("found %q, expected '}'", err)
	}

	// expecting a '}'
	if d, ok := currToken.(json.Delim); !ok || d.String() != "}" {
		return fmt.Errorf("found %q, expected '}'", d.String())
	}

	return nil
}

func parseJSONArray(dec *json.Decoder, currToken json.Token, buf *ValueBuffer) error {
	// expecting a '['
	if d, ok := currToken.(json.Delim); !ok || d.String() != "[" {
		return fmt.Errorf("found %q, expected '['", d.String())
	}

	for dec.More() {
		v, err := parseJSONValue(dec)
		if err != nil {
			return err
		}
		*buf = buf.Append(v)
	}

	// expecting ']'
	t, err := dec.Token()
	if err != nil {
		return err
	}
	if d, ok := t.(json.Delim); !ok || d != ']' {
		return fmt.Errorf("found %q, expected ']'", t)
	}

	return nil
}

// IteratorToJSON encodes all the documents of an iterator to JSON stream.
func IteratorToJSON(w io.Writer, s Iterator) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")

	return s.Iterate(func(d Document) error {
		return enc.Encode(jsonDocument{d})
	})
}

// IteratorToJSONArray encodes all the documents of an iterator to a JSON array.
func IteratorToJSONArray(w io.Writer, s Iterator) error {
	buf := bufio.NewWriter(w)

	buf.WriteByte('[')

	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")

	first := true
	err := s.Iterate(func(d Document) error {
		if !first {
			buf.WriteByte(',')
		} else {
			first = false
		}

		return ToJSON(buf, d)
	})
	if err != nil {
		return err
	}

	buf.WriteByte(']')
	return buf.Flush()
}

// Compare compares two values performing best-effort comparisons
// Returns > 0 if this value can be considered bigger
// Returns < 0 if this value can be considered smaller
// Returns 0 if values can be considered equal
func (v Value) Compare(u Value) int {
	if v.Type == NullValue && u.Type == NullValue {
		return 0
	}
	// Null is always less than non-null
	if v.Type == NullValue {
		return -1
	}
	if u.Type == NullValue {
		return 1
	}

	un := v.Type.IsNumber() || v.Type == BoolValue
	vn := u.Type.IsNumber() || u.Type == BoolValue

	// if any of the values is a number, perform a best effort numeric comparison
	if un || vn {
		var vf float64
		var uf float64
		if un {
			vf, _ = v.ConvertToFloat64()
		} else {
			vf, _ = strconv.ParseFloat(v.String(), 64)
		}
		if vn {
			uf, _ = u.ConvertToFloat64()
		} else {
			uf, _ = strconv.ParseFloat(u.String(), 64)
		}
		return int(vf - uf)
	}

	// compare byte arrays and strings
	if (v.Type == TextValue || v.Type == BlobValue) && (u.Type == TextValue || u.Type == BlobValue) {
		bv, _ := v.ConvertToBlob()
		bu, _ := u.ConvertToBlob()
		return bytes.Compare(bv, bu)
	}

	// if all else fails, compare string representation of values
	return strings.Compare(v.String(), u.String())
}
