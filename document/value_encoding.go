package document

import (
	"errors"
	"io"

	"github.com/genjidb/genji/pkg/nsb"
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
	return errors.New("cannot encode type " + v.Type.String() + " as key")
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
		err = ve.appendArray(v.V.(Array))
	case DocumentValue:
		err = ve.appendDocument(v.V.(Document))
	}
	if err != nil {
		return err
	}

	ve.buf = ve.buf[:0]

	switch v.Type {
	case BlobValue:
		ve.buf, err = nsb.AppendBase64(ve.buf, v.V.([]byte))
	case TextValue:
		text := v.V.(string)
		ve.buf, err = nsb.AppendBase64(ve.buf, []byte(text))
	case BoolValue:
		ve.buf, err = nsb.AppendBool(ve.buf, v.V.(bool)), nil
	case IntegerValue:
		ve.buf, err = nsb.AppendIntNumber(ve.buf, v.V.(int64))
	case DoubleValue:
		ve.buf, err = nsb.AppendFloatNumber(ve.buf, v.V.(float64))
	}

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

		ve.buf, err = nsb.AppendBase64(ve.buf[:0], []byte(field))
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
