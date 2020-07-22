// +build !wasm

package document

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// NewFromJSON creates a document from a JSON object.
func NewFromJSON(data []byte) (Document, error) {
	var fb FieldBuffer
	err := json.Unmarshal(data, &fb)
	if err != nil {
		return nil, err
	}
	return &fb, nil
}

// MarshalJSON implements the json.Marshaler interface.
func (v Value) MarshalJSON() ([]byte, error) {
	var x interface{}

	switch v.Type {
	case DocumentValue:
		d, err := v.ConvertToDocument()
		if err != nil {
			return nil, err
		}
		return jsonDocument{d}.MarshalJSON()
	case ArrayValue:
		a, err := v.ConvertToArray()
		if err != nil {
			return nil, err
		}
		return jsonArray{a}.MarshalJSON()
	case TextValue:
		s, err := v.ConvertToString()
		if err != nil {
			return nil, err
		}
		x = s
	case DurationValue:
		d, err := v.ConvertToDuration()
		if err != nil {
			return nil, err
		}
		x = d.String()
	default:
		x = v.V
	}

	var buf bytes.Buffer
	// json.Marshal uses HTML escaping by default
	// which causes characters like > to be transformed
	// into \u003c.
	// to disable that we need to use json.Encoder instead
	// and call SetEscapeHTML with false.
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	err := enc.Encode(x)
	if err != nil {
		return nil, err
	}

	// json.Encoder always terminates each value with a newline,
	// we need to remove it.
	return buf.Bytes()[:buf.Len()-1], nil
}

// String returns a string representation of the value. It implements the fmt.Stringer interface.
func (v Value) String() string {
	switch v.Type {
	case NullValue:
		return "NULL"
	case TextValue:
		return strconv.Quote(string(v.V.([]byte)))
	case BlobValue, DurationValue:
		return fmt.Sprintf("%v", v.V)
	}

	d, _ := v.MarshalJSON()
	return string(d)
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

// MarshalJSON implements the json.Marshaler interface.
func (vb *ValueBuffer) MarshalJSON() ([]byte, error) {
	return json.Marshal(jsonArray{Array: vb})
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

// MarshalJSON implements the json.Marshaler interface.
func (m mapDocument) MarshalJSON() ([]byte, error) {
	return json.Marshal(jsonDocument{Document: m})
}

// MarshalJSON implements the json.Marshaler interface.
func (m structDocument) MarshalJSON() ([]byte, error) {
	return json.Marshal(jsonDocument{Document: m})
}

// ToJSON encodes d to w in JSON.
func ToJSON(w io.Writer, d Document) error {
	buf, err := jsonDocument{d}.MarshalJSON()
	if err != nil {
		return err
	}

	_, err = w.Write(buf)
	return err
}

// ArrayToJSON encodes a to w in JSON.
func ArrayToJSON(w io.Writer, a Array) error {
	buf, err := jsonArray{a}.MarshalJSON()
	if err != nil {
		return err
	}

	_, err = w.Write(buf)
	return err
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
			buf.WriteString(", ")
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
			buf.WriteString(", ")
		}
		notFirst = true

		buf.WriteString(strconv.Quote(f))
		buf.WriteString(": ")

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

			return NewDoubleValue(f), nil
		}

		return NewIntegerValue(int64(i)), nil
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
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")

	return s.Iterate(func(d Document) error {
		return enc.Encode(jsonDocument{d})
	})
}

// IteratorToJSONArray encodes all the documents of an iterator to a JSON array.
func IteratorToJSONArray(w io.Writer, s Iterator) error {
	buf := bufio.NewWriter(w)

	buf.WriteByte('[')

	first := true
	err := s.Iterate(func(d Document) error {
		if !first {
			buf.WriteString(", ")
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
		bv, _ := v.ConvertToBytes()
		bu, _ := u.ConvertToBytes()
		return bytes.Compare(bv, bu)
	}

	// if all else fails, compare string representation of values
	return strings.Compare(v.String(), u.String())
}
