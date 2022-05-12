package encoding

import (
	"io"
	"sync"

	"github.com/cockroachdb/errors"
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

var pool = sync.Pool{
	New: func() interface{} {
		return (*[]byte)(nil)
	},
}

func getBuffer() []byte {
	buf := pool.Get().(*[]byte)
	if buf == nil {
		return nil
	}

	return *buf
}

func getCleanBuffer() []byte {
	buf := getBuffer()
	if len(buf) != 0 {
		buf = buf[:0]
	}
	return buf
}

func putBuffer(buf []byte) {
	pool.Put(&buf)
}

// EncodeValue encodes v to the writer.
func EncodeValue(w io.Writer, v types.Value) error {
	if ev, ok := v.(*EncodedValue); ok {
		_, err := w.Write(*ev)
		return err
	}

	buf, err := encode(getCleanBuffer(), v)
	if err != nil {
		return err
	}
	_, err = w.Write(buf)
	putBuffer(buf)
	return err
}

func encode(buf []byte, v types.Value) ([]byte, error) {
	buf = append(buf, byte(v.Type()))

	var err error
	if v.V() == nil {
		return buf, nil
	}

	switch v.Type() {
	case types.NullValue:
		return buf, nil
	case types.ArrayValue:
		return appendArray(buf, v.V().(types.Array))
	case types.DocumentValue:
		return appendDocument(buf, v.V().(types.Document))
	}

	switch v.Type() {
	case types.BlobValue:
		buf, err = AppendBase64(buf, v.V().([]byte))
	case types.TextValue:
		text := v.V().(string)
		buf, err = AppendBase64(buf, []byte(text))
	case types.BooleanValue:
		buf, err = AppendBool(buf, v.V().(bool)), nil
	case types.IntegerValue:
		buf = AppendInt64(buf, v.V().(int64))
	case types.DoubleValue:
		buf = AppendFloat64(buf, v.V().(float64))
	default:
		panic("cannot encode type " + v.Type().String() + " as key")
	}
	return buf, err
}

// appendArray encodes an array into a sort-ordered binary representation.
func appendArray(buf []byte, a types.Array) ([]byte, error) {
	err := a.Iterate(func(i int, value types.Value) error {
		if i > 0 {
			buf = append(buf, ArrayValueDelim)
		}

		var err error
		buf, err = encode(buf, value)
		return err
	})
	if err != nil {
		return nil, err
	}

	buf = append(buf, ArrayEnd)
	return buf, nil
}

// appendDocument encodes a document into a sort-ordered binary representation.
func appendDocument(buf []byte, d types.Document) ([]byte, error) {
	l := len(buf)

	// prevent duplicate field names
	fieldNames := make(map[string]bool)

	err := d.Iterate(func(field string, value types.Value) error {
		var err error

		if fieldNames[field] {
			return errors.New("duplicate field name: " + field)
		}

		fieldNames[field] = true

		// encode the field as text
		buf = append(buf, byte(types.TextValue))
		buf, err = AppendBase64(buf, []byte(field))
		if err != nil {
			return err
		}

		buf = append(buf, DocumentValueDelim)

		buf, err = encode(buf, value)
		if err != nil {
			return err
		}

		buf = append(buf, DocumentValueDelim)

		return nil
	})
	if err != nil {
		return nil, err
	}

	// replace the last delimiter with the end marker
	if len(buf) != l {
		buf[len(buf)-1] = DocumentEnd
	}

	return buf, nil
}
