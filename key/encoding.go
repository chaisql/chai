// Package key provides types and functions to encode and decode keys.
//
// Encoding keys
//
// Each type is encoded in a way that allows ordering to be preserved. That way, if vA < vB,
// where vA and vB are two unencoded values of the same type, then eA < eB, where eA and eB
// are the respective encoded values of vA and vB.
package key

import (
	"bytes"
	"errors"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/pkg/nsb"
)

const arrayValueDelim = 0x1f
const arrayEnd = 0x1e
const documentValueDelim = 0x1c
const documentEnd = 0x1d

// AppendArray encodes an array into a sort-ordered binary representation.
func AppendArray(buf []byte, a document.Array) ([]byte, error) {
	err := a.Iterate(func(i int, value document.Value) error {
		var err error

		if i > 0 {
			buf = append(buf, arrayValueDelim)
		}

		buf, err = AppendValue(buf, value)
		if err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	buf = append(buf, arrayEnd)

	return buf, nil
}

func decodeValue(data []byte, delim, end byte) (document.Value, int, error) {
	t := document.ValueType(data[0])
	i := 1

	switch t {
	case document.ArrayValue:
		a, n, err := decodeArray(data[i:])
		i += n
		if err != nil {
			return document.Value{}, i, err
		}
		return document.NewArrayValue(a), i, nil
	case document.DocumentValue:
		d, n, err := decodeDocument(data[i:])
		i += n
		if err != nil {
			return document.Value{}, i, err
		}
		return document.NewDocumentValue(d), i, nil
	case document.NullValue:
	case document.BoolValue:
		i++
	case document.DoubleValue:
		i += 16
	case document.BlobValue, document.TextValue:
		for i < len(data) && data[i] != delim && data[i] != end {
			i++
		}
	default:
		return document.Value{}, 0, errors.New("invalid type character")
	}

	v, err := DecodeValue(data[:i])
	return v, i, err
}

// DecodeArray decodes an array.
func DecodeArray(data []byte) (document.Array, error) {
	a, _, err := decodeArray(data)
	return a, err
}

func decodeArray(data []byte) (document.Array, int, error) {
	var vb document.ValueBuffer

	var readCount int
	for len(data) > 0 && data[0] != arrayEnd {
		v, i, err := decodeValue(data, arrayValueDelim, arrayEnd)
		if err != nil {
			return nil, i, err
		}

		vb = vb.Append(v)

		// skip the delimiter
		if data[i] == arrayValueDelim {
			i++
		}

		readCount += i

		data = data[i:]
	}

	// skip the array end character
	readCount++

	return vb, readCount, nil
}

// AppendDocument encodes a document into a sort-ordered binary representation.
func AppendDocument(buf []byte, d document.Document) ([]byte, error) {
	var i int
	err := d.Iterate(func(field string, value document.Value) error {
		var err error

		if i > 0 {
			buf = append(buf, documentValueDelim)
		}

		buf, err = nsb.AppendBase64(buf, []byte(field))
		if err != nil {
			return err
		}

		buf = append(buf, documentValueDelim)

		buf, err = AppendValue(buf, value)
		if err != nil {
			return err
		}

		i++
		return nil
	})
	if err != nil {
		return nil, err
	}

	buf = append(buf, documentEnd)

	return buf, nil
}

// DecodeDocument decodes a document.
func DecodeDocument(data []byte) (document.Document, error) {
	a, _, err := decodeDocument(data)
	return a, err
}

func decodeDocument(data []byte) (document.Document, int, error) {
	var fb document.FieldBuffer

	var readCount int
	for len(data) > 0 && data[0] != documentEnd {
		i := 0

		for i < len(data) && data[i] != documentValueDelim {
			i++
		}

		field, err := nsb.DecodeBase64(data[:i])
		if err != nil {
			return nil, 0, err
		}

		// skip the delimiter
		i++

		if i >= len(data) {
			return nil, 0, errors.New("invalid end of input")
		}

		readCount += i

		data = data[i:]

		v, i, err := decodeValue(data, documentValueDelim, documentEnd)
		if err != nil {
			return nil, i, err
		}

		fb.Add(string(field), v)

		// skip the delimiter
		if data[i] == documentValueDelim {
			i++
		}

		readCount += i

		data = data[i:]
	}

	// skip the document end character
	readCount++

	return &fb, readCount, nil
}

// AppendValue encodes a value as a key.
func AppendValue(buf []byte, v document.Value) ([]byte, error) {
	if v.Type == document.IntegerValue || v.Type == document.DoubleValue {
		buf = append(buf, byte(document.DoubleValue))
	} else {
		buf = append(buf, byte(v.Type))
	}

	switch v.Type {
	case document.BlobValue:
		return nsb.AppendBase64(buf, v.V.([]byte))
	case document.TextValue:
		text := v.V.(string)
		return nsb.AppendBase64(buf, []byte(text))
	case document.BoolValue:
		return nsb.AppendBool(buf, v.V.(bool)), nil
	case document.IntegerValue:
		return nsb.AppendIntNumber(buf, v.V.(int64))
	case document.DoubleValue:
		return nsb.AppendFloatNumber(buf, v.V.(float64))
	case document.NullValue:
		return buf, nil
	case document.ArrayValue:
		return AppendArray(buf, v.V.(document.Array))
	case document.DocumentValue:
		return AppendDocument(buf, v.V.(document.Document))
	}

	return nil, errors.New("cannot encode type " + v.Type.String() + " as key")
}

// DecodeValue takes some encoded data and decodes it to the target type t.
func DecodeValue(data []byte) (document.Value, error) {
	t := document.ValueType(data[0])
	data = data[1:]

	switch t {
	case document.BlobValue:
		t, err := nsb.DecodeBase64(data)
		if err != nil {
			return document.Value{}, err
		}
		return document.NewBlobValue(t), nil
	case document.TextValue:
		t, err := nsb.DecodeBase64(data)
		if err != nil {
			return document.Value{}, err
		}
		return document.NewTextValue(string(t)), nil
	case document.BoolValue:
		return document.NewBoolValue(nsb.DecodeBool(data)), nil
	case document.DoubleValue:
		if bytes.Equal(data[8:], []byte{0, 0, 0, 0, 0, 0, 0, 0}) {
			x, err := nsb.DecodeInt64(data[:8])
			if err != nil {
				return document.Value{}, err
			}
			return document.NewIntegerValue(x), nil
		}
		x, err := nsb.DecodeFloat64(data[8:])
		if err != nil {
			return document.Value{}, err
		}
		return document.NewDoubleValue(x), nil
	case document.NullValue:
		return document.NewNullValue(), nil
	case document.ArrayValue:
		a, err := DecodeArray(data)
		if err != nil {
			return document.Value{}, err
		}
		return document.NewArrayValue(a), nil
	case document.DocumentValue:
		d, err := DecodeDocument(data)
		if err != nil {
			return document.Value{}, err
		}
		return document.NewDocumentValue(d), nil
	}

	return document.Value{}, errors.New("unknown type")
}

// Append encodes a value of the type t as a key.
// The encoded key doesn't include type information.
func Append(buf []byte, t document.ValueType, v interface{}) ([]byte, error) {
	switch t {
	case document.BlobValue:
		return append(buf, v.([]byte)...), nil
	case document.TextValue:
		return append(buf, v.(string)...), nil
	case document.BoolValue:
		return nsb.AppendBool(buf, v.(bool)), nil
	case document.IntegerValue:
		return nsb.AppendInt64(buf, v.(int64)), nil
	case document.DoubleValue:
		return nsb.AppendFloat64(buf, v.(float64)), nil
	case document.NullValue:
		return buf, nil
	case document.ArrayValue:
		return AppendArray(buf, v.(document.Array))
	case document.DocumentValue:
		return AppendDocument(buf, v.(document.Document))
	}

	return nil, errors.New("cannot encode type " + t.String() + " as key")
}

// Decode takes some encoded data and decodes it to the target type t.
func Decode(t document.ValueType, data []byte) (document.Value, error) {
	switch t {
	case document.BlobValue:
		return document.NewBlobValue(data), nil
	case document.TextValue:
		return document.NewTextValue(string(data)), nil
	case document.BoolValue:
		return document.NewBoolValue(nsb.DecodeBool(data)), nil
	case document.IntegerValue:
		x, err := nsb.DecodeInt64(data)
		if err != nil {
			return document.Value{}, err
		}

		return document.NewIntegerValue(x), nil
	case document.DoubleValue:
		x, err := nsb.DecodeFloat64(data)
		if err != nil {
			return document.Value{}, err
		}
		return document.NewDoubleValue(x), nil
	case document.NullValue:
		return document.NewNullValue(), nil
	case document.ArrayValue:
		a, err := DecodeArray(data)
		if err != nil {
			return document.Value{}, err
		}
		return document.NewArrayValue(a), nil
	case document.DocumentValue:
		d, err := DecodeDocument(data)
		if err != nil {
			return document.Value{}, err
		}
		return document.NewDocumentValue(d), nil
	}

	return document.Value{}, errors.New("unknown type")
}
