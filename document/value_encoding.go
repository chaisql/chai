package document

import (
	"bytes"
	"errors"
	"io"

	"github.com/genjidb/genji/binarysort"
)

const (
	arrayValueDelim    = 0x1f
	arrayEnd           = 0x1e
	documentValueDelim = 0x1c
	documentEnd        = 0x1d
)

// ValueEncoder encodes sort-ordered representations of values.
// Type information is encoded alongside each value.
type ValueEncoder struct {
	w io.Writer

	buf []byte
}

// NewValueEncoder creates a ValueEncoder that writes to w.
func NewValueEncoder(w io.Writer) *ValueEncoder {
	return &ValueEncoder{
		w: w,
	}
}

func (ve *ValueEncoder) append(data ...byte) error {
	_, err := ve.w.Write(data)
	return err
}

// Encode v to the writer.
func (ve *ValueEncoder) Encode(v Value) error {
	return ve.appendValue(v)
}

func (ve *ValueEncoder) appendValue(v Value) error {
	var err error

	if v.Type.IsNumber() {
		err = ve.append(byte(DoubleValue))
	} else {
		err = ve.append(byte(v.Type))
	}
	if err != nil {
		return err
	}

	switch v.Type {
	case NullValue:
		return nil
	case ArrayValue:
		return ve.appendArray(v.V.(Array))
	case DocumentValue:
		return ve.appendDocument(v.V.(Document))
	}

	ve.buf = ve.buf[:0]

	switch v.Type {
	case BlobValue:
		ve.buf, err = binarysort.AppendBase64(ve.buf, v.V.([]byte))
	case TextValue:
		text := v.V.(string)
		ve.buf, err = binarysort.AppendBase64(ve.buf, []byte(text))
	case BoolValue:
		ve.buf, err = binarysort.AppendBool(ve.buf, v.V.(bool)), nil
	case IntegerValue:
		ve.buf, err = binarysort.AppendIntNumber(ve.buf, v.V.(int64))
	case DoubleValue:
		ve.buf, err = binarysort.AppendFloatNumber(ve.buf, v.V.(float64))
	default:
		return errors.New("cannot encode type " + v.Type.String() + " as key")
	}
	if err != nil {
		return err
	}
	ve.append(ve.buf...)

	return err
}

// appendArray encodes an array into a sort-ordered binary representation.
func (ve *ValueEncoder) appendArray(a Array) error {
	err := a.Iterate(func(i int, value Value) error {
		if i > 0 {
			err := ve.append(arrayValueDelim)
			if err != nil {
				return err
			}
		}

		return ve.appendValue(value)
	})
	if err != nil {
		return err
	}

	return ve.append(arrayEnd)
}

// appendDocument encodes a document into a sort-ordered binary representation.
func (ve *ValueEncoder) appendDocument(d Document) error {
	var i int
	err := d.Iterate(func(field string, value Value) error {
		var err error

		if i > 0 {
			err = ve.append(documentValueDelim)
			if err != nil {
				return err
			}
		}

		ve.buf, err = binarysort.AppendBase64(ve.buf[:0], []byte(field))
		if err != nil {
			return err
		}
		err = ve.append(ve.buf...)
		if err != nil {
			return err
		}

		err = ve.append(documentValueDelim)
		if err != nil {
			return err
		}

		err = ve.appendValue(value)
		if err != nil {
			return err
		}

		i++
		return nil
	})
	if err != nil {
		return err
	}

	return ve.append(documentEnd)
}

// decodeValue decodes a value encoded with ValueEncoder.
func decodeValue(data []byte) (Value, error) {
	t := ValueType(data[0])
	data = data[1:]

	switch t {
	case NullValue:
		return NewNullValue(), nil
	case BlobValue:
		t, err := binarysort.DecodeBase64(data)
		if err != nil {
			return Value{}, err
		}
		return NewBlobValue(t), nil
	case TextValue:
		t, err := binarysort.DecodeBase64(data)
		if err != nil {
			return Value{}, err
		}
		return NewTextValue(string(t)), nil
	case BoolValue:
		return NewBoolValue(binarysort.DecodeBool(data)), nil
	case DoubleValue:
		if bytes.Equal(data[8:], []byte{0, 0, 0, 0, 0, 0, 0, 0}) {
			x, err := binarysort.DecodeInt64(data[:8])
			if err != nil {
				return Value{}, err
			}
			return NewIntegerValue(x), nil
		}
		x, err := binarysort.DecodeFloat64(data[8:])
		if err != nil {
			return Value{}, err
		}
		return NewDoubleValue(x), nil
	case ArrayValue:
		a, _, err := decodeArray(data)
		if err != nil {
			return Value{}, err
		}
		return NewArrayValue(a), nil
	case DocumentValue:
		d, _, err := decodeDocument(data)
		if err != nil {
			return Value{}, err
		}
		return NewDocumentValue(d), nil
	}

	return Value{}, errors.New("unknown type")
}

func decodeValueUntil(data []byte, delim, end byte) (Value, int, error) {
	t := ValueType(data[0])
	i := 1

	switch t {
	case ArrayValue:
		a, n, err := decodeArray(data[i:])
		i += n
		if err != nil {
			return Value{}, i, err
		}
		return NewArrayValue(a), i, nil
	case DocumentValue:
		d, n, err := decodeDocument(data[i:])
		i += n
		if err != nil {
			return Value{}, i, err
		}
		return NewDocumentValue(d), i, nil
	case NullValue:
	case BoolValue:
		i++
	case DoubleValue:
		i += 16
	case BlobValue, TextValue:
		for i < len(data) && data[i] != delim && data[i] != end {
			i++
		}
	default:
		return Value{}, 0, errors.New("invalid type character")
	}

	v, err := decodeValue(data[:i])
	return v, i, err
}

func decodeArray(data []byte) (Array, int, error) {
	var vb ValueBuffer

	var readCount int
	for len(data) > 0 && data[0] != arrayEnd {
		v, i, err := decodeValueUntil(data, arrayValueDelim, arrayEnd)
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

func decodeDocument(data []byte) (Document, int, error) {
	var fb FieldBuffer

	var readCount int
	for len(data) > 0 && data[0] != documentEnd {
		i := 0

		for i < len(data) && data[i] != documentValueDelim {
			i++
		}

		field, err := binarysort.DecodeBase64(data[:i])
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

		v, i, err := decodeValueUntil(data, documentValueDelim, documentEnd)
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
