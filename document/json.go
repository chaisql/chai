package document

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"strconv"
)

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

		buf.WriteByte('"')
		buf.WriteString(f)
		buf.WriteString(`":`)

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
		return NewStringValue(tt), nil
	case bool:
		return NewBoolValue(tt), nil
	case nil:
		return NewNullValue(), nil
	case json.Number:
		i, err := tt.Int64()
		if err != nil {
			// if it's too big to fit in an int64, perhaps it can fit in a uint64
			ui, err := strconv.ParseUint(tt.String(), 10, 64)
			if err == nil {
				return NewUint64Value(ui), nil
			}

			// let's try parsing this as a floating point number
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
