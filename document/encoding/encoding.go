// Package encoding provides types and functions to encode and decode documents and values.
//
// Encoding values
//
// Each type is encoded in a way that allows ordering to be preserved. That way, if vA < vB,
// where vA and vB are two unencoded values of the same type, then eA < eB, where eA and eB
// are the respective encoded values of vA and vB.
package encoding

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"
	"time"

	"github.com/asdine/genji/document"
)

// EncodeBlob takes a blob and returns it.
// It is present to ease code generation.
func EncodeBlob(x []byte) []byte {
	return x
}

// DecodeBlob takes a byte slice and returns it.
// It is present to ease code generation.
func DecodeBlob(buf []byte) ([]byte, error) {
	return buf, nil
}

// EncodeText takes a string and returns its binary representation.
func EncodeText(x string) []byte {
	return []byte(x)
}

// DecodeText takes a byte slice and decodes it into a string.
func DecodeText(buf []byte) (string, error) {
	return string(buf), nil
}

// EncodeBool takes a bool and returns its binary representation.
func EncodeBool(x bool) []byte {
	if x {
		return []byte{1}
	}
	return []byte{0}
}

// DecodeBool takes a byte slice and decodes it into a boolean.
func DecodeBool(buf []byte) (bool, error) {
	if len(buf) != 1 {
		return false, errors.New("cannot decode buffer to bool")
	}
	return buf[0] == 1, nil
}

// EncodeUint takes an uint and returns its binary representation.
func EncodeUint(x uint) []byte {
	return EncodeUint64(uint64(x))
}

// DecodeUint takes a byte slice and decodes it into a uint.
func DecodeUint(buf []byte) (uint, error) {
	x, err := DecodeUint64(buf)
	return uint(x), err
}

// EncodeUint8 takes an uint8 and returns its binary representation.
func EncodeUint8(x uint8) []byte {
	return []byte{x}
}

// DecodeUint8 takes a byte slice and decodes it into a uint8.
func DecodeUint8(buf []byte) (uint8, error) {
	if len(buf) == 0 {
		return 0, errors.New("cannot decode buffer to uint8")
	}

	return buf[0], nil
}

// EncodeUint16 takes an uint16 and returns its binary representation.
func EncodeUint16(x uint16) []byte {
	var buf [2]byte
	binary.BigEndian.PutUint16(buf[:], x)
	return buf[:]
}

// DecodeUint16 takes a byte slice and decodes it into a uint16.
func DecodeUint16(buf []byte) (uint16, error) {
	if len(buf) < 2 {
		return 0, errors.New("cannot decode buffer to uint16")
	}

	return binary.BigEndian.Uint16(buf), nil
}

// EncodeUint32 takes an uint32 and returns its binary representation.
func EncodeUint32(x uint32) []byte {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], x)
	return buf[:]
}

// DecodeUint32 takes a byte slice and decodes it into a uint32.
func DecodeUint32(buf []byte) (uint32, error) {
	if len(buf) < 4 {
		return 0, errors.New("cannot decode buffer to uint32")
	}

	return binary.BigEndian.Uint32(buf), nil
}

// EncodeUint64 takes an uint64 and returns its binary representation.
func EncodeUint64(x uint64) []byte {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], x)
	return buf[:]
}

// DecodeUint64 takes a byte slice and decodes it into a uint64.
func DecodeUint64(buf []byte) (uint64, error) {
	if len(buf) < 8 {
		return 0, errors.New("cannot decode buffer to uint64")
	}

	return binary.BigEndian.Uint64(buf), nil
}

// EncodeInt takes an int and returns its binary representation.
func EncodeInt(x int) []byte {
	return EncodeInt64(int64(x))
}

// DecodeInt takes a byte slice and decodes it into an int.
func DecodeInt(buf []byte) (int, error) {
	x, err := DecodeInt64(buf)
	return int(x), err
}

// EncodeInt8 takes an int8 and returns its binary representation.
func EncodeInt8(x int8) []byte {
	return []byte{uint8(x + math.MaxInt8 + 1)}
}

// DecodeInt8 takes a byte slice and decodes it into an int8.
func DecodeInt8(buf []byte) (int8, error) {
	return int8(buf[0] - math.MaxInt8 - 1), nil
}

// EncodeInt16 takes an int16 and returns its binary representation.
func EncodeInt16(x int16) []byte {
	var buf [2]byte

	binary.BigEndian.PutUint16(buf[:], uint16(x)+math.MaxInt16+1)
	return buf[:]
}

// DecodeInt16 takes a byte slice and decodes it into an int16.
func DecodeInt16(buf []byte) (int16, error) {
	x, err := DecodeUint16(buf)
	x -= math.MaxInt16 + 1
	return int16(x), err
}

// EncodeInt32 takes an int32 and returns its binary representation.
func EncodeInt32(x int32) []byte {
	var buf [4]byte

	binary.BigEndian.PutUint32(buf[:], uint32(x)+math.MaxInt32+1)
	return buf[:]
}

// DecodeInt32 takes a byte slice and decodes it into an int32.
func DecodeInt32(buf []byte) (int32, error) {
	x, err := DecodeUint32(buf)
	x -= math.MaxInt32 + 1
	return int32(x), err
}

// EncodeInt64 takes an int64 and returns its binary representation.
func EncodeInt64(x int64) []byte {
	var buf [8]byte

	binary.BigEndian.PutUint64(buf[:], uint64(x)+math.MaxInt64+1)
	return buf[:]
}

// DecodeInt64 takes a byte slice and decodes it into an int64.
func DecodeInt64(buf []byte) (int64, error) {
	x, err := DecodeUint64(buf)
	x -= math.MaxInt64 + 1
	return int64(x), err
}

// EncodeFloat64 takes an float64 and returns its binary representation.
func EncodeFloat64(x float64) []byte {
	fb := math.Float64bits(x)
	if x >= 0 {
		fb ^= 1 << 63
	} else {
		fb ^= 1<<64 - 1
	}
	return EncodeUint64(fb)
}

// DecodeFloat64 takes a byte slice and decodes it into an float64.
func DecodeFloat64(buf []byte) (float64, error) {
	x := binary.BigEndian.Uint64(buf)

	if (x & (1 << 63)) != 0 {
		x ^= 1 << 63
	} else {
		x ^= 1<<64 - 1
	}
	return math.Float64frombits(x), nil
}

// EncodeDocument takes a document and encodes it using the encoding.Format type.
func EncodeDocument(d document.Document) ([]byte, error) {
	if ec, ok := d.(EncodedDocument); ok {
		return ec, nil
	}

	var format Format

	var offset uint64
	var dataList [][]byte

	err := d.Iterate(func(f string, v document.Value) error {
		data, err := EncodeValue(v)
		if err != nil {
			return err
		}

		format.Header.FieldHeaders = append(format.Header.FieldHeaders, FieldHeader{
			NameSize:   uint64(len(f)),
			NameString: f,
			Type:       uint64(v.Type),
			Size:       uint64(len(data)),
			Offset:     offset,
		})

		offset += uint64(len(data))
		dataList = append(dataList, data)
		return nil
	})
	if err != nil {
		return nil, err
	}

	var buf bytes.Buffer
	_, err = format.Header.WriteTo(&buf)
	if err != nil {
		return nil, err
	}

	buf.Grow(format.Header.BodySize())

	for _, data := range dataList {
		_, err = buf.Write(data)
		if err != nil {
			return nil, err
		}
	}

	return buf.Bytes(), nil
}

// DecodeDocument takes a byte slice and returns a lazily decoded document.
// If buf is malformed, an error will be returned when calling one of the document method.
func DecodeDocument(buf []byte) document.Document {
	return EncodedDocument(buf)
}

// EncodeValue encodes any value to a binary representation.
func EncodeValue(v document.Value) ([]byte, error) {
	switch v.Type {
	case document.DocumentValue:
		d, err := v.ConvertToDocument()
		if err != nil {
			return nil, err
		}
		return EncodeDocument(d)
	case document.ArrayValue:
		a, err := v.ConvertToArray()
		if err != nil {
			return nil, err
		}
		return EncodeArray(a)
	case document.BlobValue:
		x, err := v.ConvertToBlob()
		if err != nil {
			return nil, err
		}
		return EncodeBlob(x), nil
	case document.TextValue:
		x, err := v.ConvertToText()
		if err != nil {
			return nil, err
		}
		return EncodeText(x), nil
	case document.BoolValue:
		x, err := v.ConvertToBool()
		if err != nil {
			return nil, err
		}
		return EncodeBool(x), nil
	case document.Int8Value:
		return EncodeInt8(v.V.(int8)), nil
	case document.Int16Value:
		return EncodeInt16(v.V.(int16)), nil
	case document.Int32Value:
		return EncodeInt32(v.V.(int32)), nil
	case document.Int64Value:
		return EncodeInt64(v.V.(int64)), nil
	case document.Float64Value:
		return EncodeFloat64(v.V.(float64)), nil
	case document.DurationValue:
		return EncodeInt64(int64(v.V.(time.Duration))), nil
	case document.NullValue:
		return nil, nil
	}

	return nil, errors.New("unknown type")
}

// An EncodedDocument implements the document.Document interface on top of an encoded representation of a
// document.
// It is useful to avoid decoding the entire document when only a few fields are needed.
type EncodedDocument []byte

// GetByField decodes the selected field.
func (e EncodedDocument) GetByField(field string) (document.Value, error) {
	return decodeValueFromDocument(e, field)
}

// Iterate decodes each fields one by one and passes them to fn until the end of the document
// or until fn returns an error.
func (e EncodedDocument) Iterate(fn func(field string, value document.Value) error) error {
	var format Format
	err := format.Decode(e)
	if err != nil {
		return err
	}

	for _, fh := range format.Header.FieldHeaders {
		v, err := DecodeValue(document.ValueType(fh.Type), format.Body[fh.Offset:fh.Offset+fh.Size])
		if err != nil {
			return err
		}

		err = fn(string(fh.Name), v)
		if err != nil {
			return err
		}
	}

	return nil
}

// An EncodedArray implements the document.Array interface on top of an encoded representation of an
// array.
// It is useful to avoid decoding the entire array when only a few values are needed.
type EncodedArray []byte

// Iterate goes through all the values of the array and calls the given function by passing each one of them.
// If the given function returns an error, the iteration stops.
func (e EncodedArray) Iterate(fn func(i int, value document.Value) error) error {
	var format Format
	err := format.Decode(e)
	if err != nil {
		return err
	}

	for _, fh := range format.Header.FieldHeaders {
		v, err := DecodeValue(document.ValueType(fh.Type), format.Body[fh.Offset:fh.Offset+fh.Size])
		if err != nil {
			return err
		}

		i, err := DecodeInt64(fh.Name)
		if err != nil {
			return err
		}
		err = fn(int(i), v)
		if err != nil {
			return err
		}
	}

	return nil
}

// GetByIndex returns a value by index of the array.
func (e EncodedArray) GetByIndex(i int) (document.Value, error) {
	return decodeValueFromDocument(e, string(EncodeInt64(int64(i))))
}

func decodeValueFromDocument(data []byte, field string) (document.Value, error) {
	hsize, n := binary.Uvarint(data)
	if n <= 0 {
		return document.Value{}, errors.New("can't decode data")
	}

	hdata := data[n : n+int(hsize)]
	body := data[n+len(hdata):]

	// skip number of fields
	_, n = binary.Uvarint(hdata)
	if n <= 0 {
		return document.Value{}, errors.New("can't decode data")
	}
	hdata = hdata[n:]

	var fh FieldHeader
	for len(hdata) > 0 {
		n, err := fh.Decode(hdata)
		if err != nil {
			return document.Value{}, err
		}
		hdata = hdata[n:]

		if field == string(fh.Name) {
			return DecodeValue(document.ValueType(fh.Type), body[fh.Offset:fh.Offset+fh.Size])
		}
	}

	return document.Value{}, document.ErrFieldNotFound
}

// EncodeArray encodes a into its binary representation.
func EncodeArray(a document.Array) ([]byte, error) {
	var format Format

	var offset uint64
	var dataList [][]byte

	err := a.Iterate(func(i int, v document.Value) error {
		data, err := EncodeValue(v)
		if err != nil {
			return err
		}

		index := EncodeInt64(int64(i))

		format.Header.FieldHeaders = append(format.Header.FieldHeaders, FieldHeader{
			NameSize: uint64(len(index)),
			Name:     index,
			Type:     uint64(v.Type),
			Size:     uint64(len(data)),
			Offset:   offset,
		})

		offset += uint64(len(data))
		dataList = append(dataList, data)
		return nil
	})
	if err != nil {
		return nil, err
	}

	var buf bytes.Buffer
	_, err = format.Header.WriteTo(&buf)
	if err != nil {
		return nil, err
	}

	buf.Grow(format.Header.BodySize())

	for _, data := range dataList {
		_, err = buf.Write(data)
		if err != nil {
			return nil, err
		}
	}

	return buf.Bytes(), nil
}

// DecodeArray takes a byte slice and returns a lazily decoded array.
// If buf is malformed, an error will be returned when calling one of the array method.
func DecodeArray(buf []byte) document.Array {
	return EncodedArray(buf)
}

// DecodeValue takes some encoded data and decodes it to the target type t.
func DecodeValue(t document.ValueType, data []byte) (document.Value, error) {
	switch t {
	case document.DocumentValue:
		return document.NewDocumentValue(EncodedDocument(data)), nil
	case document.ArrayValue:
		return document.NewArrayValue(EncodedArray(data)), nil
	case document.BlobValue:
		x, err := DecodeBlob(data)
		if err != nil {
			return document.Value{}, err
		}
		return document.NewBlobValue(x), nil
	case document.TextValue:
		x, err := DecodeText(data)
		if err != nil {
			return document.Value{}, err
		}
		return document.NewTextValue(x), nil
	case document.BoolValue:
		x, err := DecodeBool(data)
		if err != nil {
			return document.Value{}, err
		}
		return document.NewBoolValue(x), nil
	case document.Int8Value:
		x, err := DecodeInt8(data)
		if err != nil {
			return document.Value{}, err
		}
		return document.NewInt8Value(x), nil
	case document.Int16Value:
		x, err := DecodeInt16(data)
		if err != nil {
			return document.Value{}, err
		}
		return document.NewInt16Value(x), nil
	case document.Int32Value:
		x, err := DecodeInt32(data)
		if err != nil {
			return document.Value{}, err
		}
		return document.NewInt32Value(x), nil
	case document.Int64Value:
		x, err := DecodeInt64(data)
		if err != nil {
			return document.Value{}, err
		}
		return document.NewInt64Value(x), nil
	case document.Float64Value:
		x, err := DecodeFloat64(data)
		if err != nil {
			return document.Value{}, err
		}
		return document.NewFloat64Value(x), nil
	case document.DurationValue:
		x, err := DecodeInt64(data)
		if err != nil {
			return document.Value{}, err
		}
		return document.NewDurationValue(time.Duration(x)), nil
	case document.NullValue:
		return document.NewNullValue(), nil
	}

	return document.Value{}, errors.New("unknown type")
}
