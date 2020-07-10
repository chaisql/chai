package msgpack

import (
	"fmt"
	"io"
	"time"

	"github.com/genjidb/genji/document"
	"github.com/vmihailenco/msgpack/v5"
	"github.com/vmihailenco/msgpack/v5/codes"
)

// List of custom types
const (
	DurationType int8 = 0x1
)

// Encoder encodes Genji documents and values
// in MessagePack.
type Encoder struct {
	enc *msgpack.Encoder
}

// NewEncoder creates an Encoder that writes in the given writer.
func NewEncoder(w io.Writer) *Encoder {
	enc := msgpack.GetEncoder()
	enc.Reset(w)
	enc.UseCompactInts(true)

	return &Encoder{
		enc: enc,
	}
}

// EncodeDocument encodes d as a MessagePack map.
func (e *Encoder) EncodeDocument(d document.Document) error {
	var dlen int
	var err error

	fb, ok := d.(*document.FieldBuffer)
	if ok {
		dlen = fb.Len()
	} else {
		dlen, err = document.Length(d)
		if err != nil {
			return err
		}
	}

	if err := e.enc.EncodeMapLen(dlen); err != nil {
		return err
	}

	return d.Iterate(func(f string, v document.Value) error {
		if err := e.enc.EncodeString(f); err != nil {
			return err
		}

		return e.EncodeValue(v)
	})
}

// EncodeArray encodes a as a MessagePack array.
func (e *Encoder) EncodeArray(a document.Array) error {
	var alen int
	var err error

	vb, ok := a.(document.ValueBuffer)
	if ok {
		alen = len(vb)
	} else {
		alen, err = document.ArrayLength(a)
		if err != nil {
			return err
		}
	}

	if err := e.enc.EncodeArrayLen(alen); err != nil {
		return err
	}

	return a.Iterate(func(i int, v document.Value) error {
		return e.EncodeValue(v)
	})
}

// EncodeValue encodes v based on its type.
// - document -> map
// - array -> array
// - NULL -> nil
// - text -> string
// - blob -> bytes
// - bool -> bool
// - int8 -> int8
// - int16 -> int16
// - int32 -> int32
// - int64 -> int64
// - duration -> custom type with code 0x1 and size 8
func (e *Encoder) EncodeValue(v document.Value) error {
	switch v.Type {
	case document.DocumentValue:
		d, err := v.ConvertToDocument()
		if err != nil {
			return err
		}
		return e.EncodeDocument(d)
	case document.ArrayValue:
		a, err := v.ConvertToArray()
		if err != nil {
			return err
		}
		return e.EncodeArray(a)
	case document.NullValue:
		return e.enc.EncodeNil()
	case document.TextValue:
		s, err := v.ConvertToText()
		if err != nil {
			return err
		}
		return e.enc.EncodeString(s)
	case document.BlobValue:
		b, err := v.ConvertToBlob()
		if err != nil {
			return err
		}
		return e.enc.EncodeBytes(b)
	case document.BoolValue:
		return e.enc.EncodeBool(v.V.(bool))
	case document.Int8Value:
		return e.enc.EncodeInt8(v.V.(int8))
	case document.Int16Value:
		return e.enc.EncodeInt16(v.V.(int16))
	case document.Int32Value:
		return e.enc.EncodeInt32(v.V.(int32))
	case document.Int64Value:
		return e.enc.EncodeInt64(v.V.(int64))
	case document.Float64Value:
		return e.enc.EncodeFloat64(v.V.(float64))
	case document.DurationValue:
		err := e.enc.EncodeExtHeader(DurationType, 8)
		if err != nil {
			return err
		}

		d := uint64(v.V.(time.Duration))
		var buf [8]byte
		buf[0] = byte(d >> 56)
		buf[1] = byte(d >> 48)
		buf[2] = byte(d >> 40)
		buf[3] = byte(d >> 32)
		buf[4] = byte(d >> 24)
		buf[5] = byte(d >> 16)
		buf[6] = byte(d >> 8)
		buf[7] = byte(d)

		_, err = e.enc.Writer().Write(buf[:])
		return err
	}

	return e.enc.Encode(v.V)
}

// Close puts the encoder into the pool for reuse.
func (e *Encoder) Close() {
	msgpack.PutEncoder(e.enc)
}

// Decoder decodes Genji documents and values
// from MessagePack.
type Decoder struct {
	dec *msgpack.Decoder

	curDocLen int
}

// NewDecoder creates a Decoder that reads from the given reader.
func NewDecoder(r io.Reader) *Decoder {
	dec := msgpack.GetDecoder()
	dec.Reset(r)

	return &Decoder{
		dec: dec,
	}
}

// DecodeValue reads one value from the reader and decodes it.
func (d *Decoder) DecodeValue() (v document.Value, err error) {
	c, err := d.dec.PeekCode()
	if err != nil {
		return
	}

	// decode array
	if (codes.IsFixedArray(c)) || (c == codes.Array16) || (c == codes.Array32) {
		var a document.Array
		a, err = d.DecodeArray()
		if err != nil {
			return
		}

		v = document.NewArrayValue(a)
		return
	}

	// decode document
	if (codes.IsFixedMap(c)) || (c == codes.Map16) || (c == codes.Map32) {
		var doc document.Document
		doc, err = d.DecodeDocument()
		if err != nil {
			return
		}

		v = document.NewDocumentValue(doc)
		return
	}

	// decode string
	if codes.IsString(c) {
		var s string
		s, err = d.dec.DecodeString()
		if err != nil {
			return
		}
		v.V = []byte(s)
		v.Type = document.TextValue
		return
	}

	// decode custom codes
	if codes.IsExt(c) {
		var tp int8
		tp, _, err = d.dec.DecodeExtHeader()
		if err != nil {
			return
		}

		if tp != DurationType {
			panic(fmt.Sprintf("unknown custom code %d", tp))
		}

		var buf [8]byte
		err = d.dec.ReadFull(buf[:])
		if err != nil {
			return
		}
		n := (uint64(buf[0]) << 56) |
			(uint64(buf[1]) << 48) |
			(uint64(buf[2]) << 40) |
			(uint64(buf[3]) << 32) |
			(uint64(buf[4]) << 24) |
			(uint64(buf[5]) << 16) |
			(uint64(buf[6]) << 8) |
			uint64(buf[7])
		v.V = time.Duration(n)
		v.Type = document.DurationValue
		return
	}

	// decode the rest
	switch c {
	case codes.Nil:
		err = d.dec.DecodeNil()
		if err != nil {
			return
		}
		v.Type = document.NullValue
		return
	case codes.Bin8, codes.Bin16, codes.Bin32:
		v.V, err = d.dec.DecodeBytes()
		if err != nil {
			return
		}
		v.Type = document.BlobValue
		return
	case codes.True, codes.False:
		v.V, err = d.dec.DecodeBool()
		if err != nil {
			return
		}
		v.Type = document.BoolValue
		return
	case codes.Int8:
		v.V, err = d.dec.DecodeInt8()
		if err != nil {
			return
		}
		v.Type = document.Int8Value
		return
	case codes.Int16:
		v.V, err = d.dec.DecodeInt16()
		if err != nil {
			return
		}
		v.Type = document.Int16Value
		return
	case codes.Int32:
		v.V, err = d.dec.DecodeInt32()
		if err != nil {
			return
		}
		v.Type = document.Int32Value
		return
	case codes.Int64:
		v.V, err = d.dec.DecodeInt64()
		if err != nil {
			return
		}
		v.Type = document.Int64Value
		return
	case codes.Double:
		v.V, err = d.dec.DecodeFloat64()
		if err != nil {
			return
		}
		v.Type = document.Float64Value
		return
	}

	panic("unsupported type")
}

// DecodeDocument decodes one document from the reader.
// If the document is malformed, it will not return an error.
// However, calls to Iterate or GetByField will fail.
func (d *Decoder) DecodeDocument() (document.Document, error) {
	r, err := d.dec.DecodeRaw()
	if err != nil {
		return nil, err
	}

	return EncodedDocument(r), nil
}

// DecodeArray decodes one array from the reader.
// If the array is malformed, this function will not return an error.
// However, calls to Iterate or GetByIndex will fail.
func (d *Decoder) DecodeArray() (document.Array, error) {
	r, err := d.dec.DecodeRaw()
	if err != nil {
		return nil, err
	}

	return EncodedArray(r), nil
}

// Close puts the decoder into the pool for reuse.
func (d *Decoder) Close() {
	msgpack.PutDecoder(d.dec)
}
