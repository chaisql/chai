package encoding

import (
	"io"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/internal/binarysort"
	"github.com/genjidb/genji/internal/errors"
	"github.com/genjidb/genji/types"
)

const (
	// types.ArrayValueDelim is a separator used when encoding Array in
	// binary reprsentation
	ArrayValueDelim = 0x1f
	// ArrayEnd is the final separator used when encoding Array in
	// binary reprsentation.
	ArrayEnd           = 0x1e
	DocumentValueDelim = 0x1c
	DocumentEnd        = 0x1d
)

// ValueEncoder encodes natural sort-ordered representations of values.
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
func (ve *ValueEncoder) Encode(v types.Value) error {
	return ve.appendValue(v)
}

func (ve *ValueEncoder) appendValue(v types.Value) error {
	err := ve.append(byte(v.Type()))
	if err != nil {
		return err
	}

	if v.V() == nil {
		return nil
	}

	switch v.Type() {
	case types.NullValue:
		return nil
	case types.ArrayValue:
		return ve.appendArray(v.V().(types.Array))
	case types.DocumentValue:
		return ve.appendDocument(v.V().(types.Document))
	}

	ve.buf = ve.buf[:0]

	switch v.Type() {
	case types.BlobValue:
		ve.buf, err = binarysort.AppendBase64(ve.buf, v.V().([]byte))
	case types.TextValue:
		text := v.V().(string)
		ve.buf, err = binarysort.AppendBase64(ve.buf, []byte(text))
	case types.BoolValue:
		ve.buf, err = binarysort.AppendBool(ve.buf, v.V().(bool)), nil
	case types.IntegerValue:
		ve.buf = binarysort.AppendInt64(ve.buf, v.V().(int64))
	case types.DoubleValue:
		ve.buf = binarysort.AppendFloat64(ve.buf, v.V().(float64))
	default:
		panic("cannot encode type " + v.Type().String() + " as key")
	}
	if err != nil {
		return err
	}
	ve.append(ve.buf...)

	return err
}

// appendArray encodes an array into a sort-ordered binary representation.
func (ve *ValueEncoder) appendArray(a types.Array) error {
	err := a.Iterate(func(i int, value types.Value) error {
		if i > 0 {
			err := ve.append(ArrayValueDelim)
			if err != nil {
				return err
			}
		}

		return ve.appendValue(value)
	})
	if err != nil {
		return err
	}

	return ve.append(ArrayEnd)
}

// appendDocument encodes a document into a sort-ordered binary representation.
func (ve *ValueEncoder) appendDocument(d types.Document) error {
	var i int
	err := d.Iterate(func(field string, value types.Value) error {
		var err error

		if i > 0 {
			err = ve.append(DocumentValueDelim)
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

		err = ve.append(DocumentValueDelim)
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

	return ve.append(DocumentEnd)
}

// DecodeValue decodes a value encoded with ValueEncoder.
func DecodeValue(data []byte) (types.Value, error) {
	t := types.ValueType(data[0])

	switch t {
	case types.NullValue:
		return types.NewNullValue(), nil
	case types.BlobValue:
		t, err := binarysort.DecodeBase64(data[1:])
		if err != nil {
			return nil, err
		}
		return types.NewBlobValue(t), nil
	case types.TextValue:
		t, err := binarysort.DecodeBase64(data[1:])
		if err != nil {
			return nil, err
		}
		return types.NewTextValue(string(t)), nil
	case types.BoolValue:
		b, err := binarysort.DecodeBool(data[1:])
		if err != nil {
			return nil, err
		}
		return types.NewBoolValue(b), nil
	case types.IntegerValue:
		x, err := binarysort.DecodeInt64(data[1:])
		if err != nil {
			return nil, err
		}
		return types.NewIntegerValue(x), nil
	case types.DoubleValue:
		x, err := binarysort.DecodeFloat64(data[1:])
		if err != nil {
			return nil, err
		}
		return types.NewDoubleValue(x), nil
	case types.ArrayValue:
		a, _, err := DecodeArray(data)
		if err != nil {
			return nil, err
		}
		return types.NewArrayValue(a), nil
	case types.DocumentValue:
		d, _, err := DecodeDocument(data)
		if err != nil {
			return nil, err
		}
		return types.NewDocumentValue(d), nil
	}

	return nil, errors.New("unknown type")
}

func decodeValueUntil(data []byte, delim, end byte) (types.Value, int, error) {
	t := types.ValueType(data[0])
	i := 1

	switch t {
	case types.ArrayValue:
		a, n, err := DecodeArray(data)
		i += n
		if err != nil {
			return nil, i, err
		}
		return types.NewArrayValue(a), i, nil
	case types.DocumentValue:
		d, n, err := DecodeDocument(data)
		i += n
		if err != nil {
			return nil, i, err
		}
		return types.NewDocumentValue(d), i, nil
	case types.NullValue:
	case types.BoolValue:
		i++
	case types.IntegerValue, types.DoubleValue:
		if i+8 < len(data) && (data[i+8] == delim || data[i+8] == end) {
			i += 8
		} else {
			return nil, 0, errors.New("malformed " + t.String())
		}
	case types.BlobValue, types.TextValue:
		for i < len(data) && data[i] != delim && data[i] != end {
			i++
		}
	default:
		return nil, 0, errors.New("invalid type character")
	}

	v, err := DecodeValue(data[:i])
	return v, i, err
}

func DecodeArray(data []byte) (types.Array, int, error) {
	var vb document.ValueBuffer

	// skip type
	data = data[1:]

	var readCount int
	for len(data) > 0 && data[0] != ArrayEnd {
		v, i, err := decodeValueUntil(data, ArrayValueDelim, ArrayEnd)
		if err != nil {
			return nil, i, err
		}

		vb.Append(v)

		// skip the delimiter
		if data[i] == ArrayValueDelim {
			i++
		}

		readCount += i

		data = data[i:]
	}

	// skip the array end character
	readCount++

	return &vb, readCount, nil
}

func DecodeDocument(data []byte) (types.Document, int, error) {
	var fb document.FieldBuffer

	// skip type
	data = data[1:]

	var readCount int
	for len(data) > 0 && data[0] != DocumentEnd {
		i := 0

		for i < len(data) && data[i] != DocumentValueDelim {
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

		v, i, err := decodeValueUntil(data, DocumentValueDelim, DocumentEnd)
		if err != nil {
			return nil, i, err
		}

		fb.Add(string(field), v)

		// skip the delimiter
		if data[i] == DocumentValueDelim {
			i++
		}

		readCount += i

		data = data[i:]
	}

	// skip the document end character
	readCount++

	return &fb, readCount, nil
}
