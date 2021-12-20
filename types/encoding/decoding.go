package encoding

import (
	"bytes"

	"github.com/genjidb/genji/internal/errors"
	"github.com/genjidb/genji/types"
)

func DecodeValue(data []byte) (types.Value, error) {
	e := EncodedValue(data)
	return &e, nil
}

type EncodedValue []byte

func (e *EncodedValue) Type() types.ValueType {
	return types.ValueType((*e)[0])
}

func (e *EncodedValue) V() interface{} {
	data := *e
	var err error
	switch e.Type() {
	case types.NullValue:
		return nil
	case types.BlobValue:
		buf := getBuffer()
		buf, err = DecodeBase64(buf, data[1:])
		if err != nil {
			panic(err)
		}
		return buf
	case types.TextValue:
		buf := getBuffer()
		buf, err = DecodeBase64(buf, data[1:])
		if err != nil {
			panic(err)
		}
		putBuffer(buf)
		return string(buf)
	case types.BoolValue:
		b, err := DecodeBool(data[1:])
		if err != nil {
			panic(err)
		}
		return b
	case types.IntegerValue:
		x, err := DecodeInt64(data[1:])
		if err != nil {
			panic(err)
		}
		return x
	case types.DoubleValue:
		x, err := DecodeFloat64(data[1:])
		if err != nil {
			panic(err)
		}
		return x
	case types.ArrayValue:
		enc := EncodedArray(data)
		return &enc
	case types.DocumentValue:
		enc := EncodedDocument(data)
		return &enc
	}

	panic("unreachable")
}

func (e *EncodedValue) String() string {
	return types.NewValueWith(e.Type(), e.V()).String()
}

func (e *EncodedValue) MarshalJSON() ([]byte, error) {
	return types.NewValueWith(e.Type(), e.V()).MarshalJSON()
}

func (e *EncodedValue) MarshalText() ([]byte, error) {
	return types.NewValueWith(e.Type(), e.V()).MarshalText()
}

// An EncodedDocument implements the types.Document
// interface on top of an encoded representation of a
// document.
// It is useful for avoiding decoding the entire document when
// only a few fields are needed.
type EncodedDocument []byte

var errStop = errors.New("stop")

// GetByField decodes the selected field from the buffer.
func (e *EncodedDocument) GetByField(field string) (types.Value, error) {
	// encode the field we're looking for
	buf, err := AppendBase64(getCleanBuffer(), []byte(field))
	if err != nil {
		return nil, err
	}
	defer putBuffer(buf)

	var v EncodedValue
	err = e.iterate(func(field, value []byte) error {
		// check if we found the field
		if bytes.Equal(field[1:], buf) {
			v = value
			return errStop
		}

		return nil
	})

	switch err {
	case nil:
		return nil, types.ErrFieldNotFound
	case errStop:
		return &v, nil
	default:
		return nil, err
	}
}

// Iterate decodes each fields one by one and passes them to fn
// until the end of the document or until fn returns an error.
func (e *EncodedDocument) Iterate(fn func(field string, value types.Value) error) error {
	buf := getBuffer()
	defer putBuffer(buf)

	var ev EncodedValue
	return e.iterate(func(field, value []byte) error {
		var err error
		buf, err = DecodeBase64(buf, field[1:])
		if err != nil {
			return err
		}

		ev = value
		return fn(string(buf), &ev)
	})
}

func (e *EncodedDocument) iterate(fn func(field, value []byte) error) error {
	data := *e
	// skip type
	data = data[1:]

	i := 0
	for i < len(data) {
		// skip field name
		n := skipValueUntil(data[i:], DocumentValueDelim, DocumentEnd)

		if n == 0 {
			break
		}

		field := data[i : i+n]

		// skip the delimiter
		i += n + 1

		// skip the value
		n = skipValueUntil(data[i:], DocumentValueDelim, DocumentEnd)
		value := data[i : i+n]

		err := fn(field, value)
		if err != nil {
			return err
		}

		// skip the delimiter
		i += n + 1
	}

	return nil
}

func (e *EncodedDocument) MarshalJSON() ([]byte, error) {
	return types.NewDocumentValue(e).MarshalJSON()
}

// An EncodedArray implements the types.Array interface on top of an
// encoded representation of an array.
// It is useful for avoiding decoding the entire array when
// only a few values are needed.
type EncodedArray []byte

// GetByIndex returns a value by index of the array.
func (e *EncodedArray) GetByIndex(idx int) (types.Value, error) {
	var v types.Value

	err := e.iterate(func(i int, value []byte) error {
		if i == idx {
			enc := EncodedValue(value)
			v = &enc
			return errStop
		}

		return nil
	})

	switch err {
	case nil:
		return nil, types.ErrValueNotFound
	case errStop:
		return v, nil
	default:
		return nil, err
	}
}

// Iterate goes through all the values of the array and calls the
// given function by passing each one of them.
// If the given function returns an error, the iteration stops.
func (e *EncodedArray) Iterate(fn func(i int, value types.Value) error) error {
	var ev EncodedValue

	return e.iterate(func(i int, value []byte) error {
		ev = value
		return fn(i, &ev)
	})
}

func (e *EncodedArray) iterate(fn func(i int, value []byte) error) error {
	data := *e
	// skip type
	data = data[1:]
	i := 0
	cur := 0

	for i < len(data) {
		n := skipValueUntil(data[i:], ArrayValueDelim, ArrayEnd)
		if n == 0 {
			break
		}

		err := fn(cur, data[i:i+n])
		if err != nil {
			return err
		}

		i += n

		if data[i] == ArrayEnd {
			break
		}

		// skip the delimiter
		i++

		// increment the index
		cur++
	}

	return nil
}

func (e *EncodedArray) MarshalJSON() ([]byte, error) {
	return types.NewArrayValue(e).MarshalJSON()
}

func skipValueUntil(data []byte, delim, end byte) int {
	var i int

	tp := types.ValueType(data[0])

	// skip the type
	i++
	switch tp {
	case types.NullValue:
		return i
	case types.BoolValue:
		return i + 1
	case types.BlobValue, types.TextValue:
		for i < len(data) && data[i] != delim && data[i] != end {
			i++
		}

		return i
	case types.IntegerValue, types.DoubleValue:
		// skip 8 bytes
		return i + 8
	case types.ArrayValue:
		if data[i] == ArrayEnd {
			return i + 1
		}
	LOOPARR:
		for {
			i += skipValueUntil(data[i:], ArrayValueDelim, ArrayEnd)
			switch data[i] {
			case ArrayValueDelim:
				i++
			case ArrayEnd:
				i++
				break LOOPARR
			}
		}
		return i
	case types.DocumentValue:
		if data[i] == DocumentEnd {
			return i + 1
		}
	LOOPDOC:
		for {
			// skip field
			i += skipValueUntil(data[i:], DocumentValueDelim, DocumentEnd)

			// skip value
			i += skipValueUntil(data[i:], DocumentValueDelim, DocumentEnd)

			// check if we're done
			switch data[i] {
			case DocumentValueDelim:
				i++
			case DocumentEnd:
				i++
				break LOOPDOC
			}
		}
		return i
	}

	return 0
}
