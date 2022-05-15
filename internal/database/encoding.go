package database

import (
	"bytes"
	"fmt"
	"io"

	"github.com/cockroachdb/errors"
	"github.com/genjidb/genji/document"
	errs "github.com/genjidb/genji/errors"
	"github.com/genjidb/genji/types"
	"github.com/vmihailenco/msgpack/v5"
	"github.com/vmihailenco/msgpack/v5/msgpcode"
)

// EncodeDocument validates a document against all the constraints of the table
// and encodes it.
func (t *TableInfo) EncodeDocument(tx *Transaction, w io.Writer, d types.Document) error {
	if ed, ok := d.(*EncodedDocument); ok {
		_, err := w.Write(ed.encoded)
		return err
	}

	enc := msgpack.GetEncoder()
	enc.Reset(w)
	enc.UseCompactInts(true)
	enc.UseCompactFloats(true)
	defer msgpack.PutEncoder(enc)

	return encodeDocument(tx, enc, &t.FieldConstraints, d)
}

func encodeDocument(tx *Transaction, enc *msgpack.Encoder, fcs *FieldConstraints, d types.Document) error {
	// loop over all the defined field contraints in order.
	for _, fc := range fcs.Ordered {
		// get the field from the document
		v, err := d.GetByField(fc.Field)
		if err != nil && !errors.Is(err, types.ErrFieldNotFound) {
			return err
		}

		// if the field is not found OR NULL, and the field has a default value, use the default value
		// otherwise return an error
		if v == nil {
			if fc.DefaultValue != nil {
				v, err = fc.DefaultValue.Eval(tx, d)
				if err != nil {
					return err
				}
			}
		}

		// if the field is not found OR NUL, and the field is required, return an error
		if fc.IsNotNull && (v == nil || v.Type() == types.NullValue) {
			return &errs.ConstraintViolationError{Constraint: "NOT NULL", Paths: []document.Path{document.NewPath(fc.Field)}}
		}

		if v == nil {
			v = types.NewNullValue()
		}

		// ensure the value is of the correct type
		if fc.Type != types.AnyValue {
			v, err = document.CastAs(v, fc.Type)
			if err != nil {
				return err
			}
		}

		// Encode the value only.

		if v.Type() == types.DocumentValue {
			// encode map length
			mlen := len(fc.AnonymousType.FieldConstraints.Ordered)
			if fc.AnonymousType.FieldConstraints.AllowExtraFields {
				mlen += 1
			}
			err = enc.EncodeArrayLen(mlen)
			if err != nil {
				return err
			}
			err = encodeDocument(tx, enc, &fc.AnonymousType.FieldConstraints, v.V().(types.Document))
		} else {
			err = encodeValue(enc, v)
		}
		if err != nil {
			return err
		}
	}

	// encode the extra fields, if any.
	if fcs.AllowExtraFields {
		err := encodeExtraFields(enc, fcs, d)
		if err != nil {
			return err
		}
	}

	return nil
}

func encodeExtraFields(enc *msgpack.Encoder, fcs *FieldConstraints, d types.Document) error {
	// count the number of extra fields
	extraFields := 0
	err := d.Iterate(func(field string, value types.Value) error {
		_, ok := fcs.ByField[field]
		if ok {
			return nil
		}
		extraFields++
		return nil
	})
	if err != nil {
		return err
	}

	// encode the map length
	err = enc.EncodeMapLen(extraFields)
	if err != nil {
		return err
	}

	if extraFields == 0 {
		return nil
	}

	fields := make(map[string]struct{}, extraFields)

	return d.Iterate(func(field string, value types.Value) error {
		_, ok := fcs.ByField[field]
		if ok {
			return nil
		}

		// ensure the field is not repeated
		if _, ok := fields[field]; ok {
			return errors.New("duplicate field " + field)
		}
		fields[field] = struct{}{}

		// encode the field name first
		err := enc.EncodeString(field)
		if err != nil {
			return err
		}

		// then encode the value
		return encodeValue(enc, value)
	})
}

func encodeValue(enc *msgpack.Encoder, v types.Value) error {
	switch v.Type() {
	case types.DocumentValue:
		return encodeGenericDocument(enc, v.V().(types.Document))
	case types.ArrayValue:
		return encodeArray(enc, v.V().(types.Array))
	case types.NullValue:
		return enc.EncodeNil()
	case types.TextValue:
		return enc.EncodeString(v.V().(string))
	case types.BlobValue:
		return enc.EncodeBytes(v.V().([]byte))
	case types.BooleanValue:
		return enc.EncodeBool(v.V().(bool))
	case types.IntegerValue:
		return enc.EncodeInt(v.V().(int64))
	case types.DoubleValue:
		return enc.EncodeFloat64(v.V().(float64))
	}

	panic("cannot encode type " + v.Type().String() + " as key")
}

// EncodeArray encodes a as a MessagePack array.
func encodeArray(enc *msgpack.Encoder, a types.Array) error {
	var alen int
	var err error

	vb, ok := a.(*document.ValueBuffer)
	if ok {
		alen = vb.Len()
	} else {
		alen, err = document.ArrayLength(a)
		if err != nil {
			return err
		}
	}

	if err := enc.EncodeArrayLen(alen); err != nil {
		return err
	}

	return a.Iterate(func(i int, v types.Value) error {
		return encodeValue(enc, v)
	})
}

// encodeGenericDocument encodes d as a MessagePack map.
func encodeGenericDocument(enc *msgpack.Encoder, d types.Document) error {
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

	if err := enc.EncodeMapLen(dlen); err != nil {
		return err
	}

	fields := make(map[string]struct{}, dlen)

	return d.Iterate(func(f string, v types.Value) error {
		// ensure the field is not repeated
		if _, ok := fields[f]; ok {
			return errors.New("duplicate field " + f)
		}
		fields[f] = struct{}{}

		if err := enc.EncodeString(f); err != nil {
			return err
		}

		return encodeValue(enc, v)
	})
}

type TypedCodec struct {
	Tx        *Transaction
	TableInfo *TableInfo
}

func NewCodec(tx *Transaction, tableInfo *TableInfo) *TypedCodec {
	return &TypedCodec{
		Tx:        tx,
		TableInfo: tableInfo,
	}
}

func (c *TypedCodec) Encode(w io.Writer, d types.Document) error {
	return c.TableInfo.EncodeDocument(c.Tx, w, d)
}

func (c *TypedCodec) Decode(encoded []byte) (types.Document, error) {
	return NewEncodedDocument(&c.TableInfo.FieldConstraints, encoded), nil
}

type AsIsCodec struct{}

func (c AsIsCodec) Encode(w io.Writer, d types.Document) error {
	enc := msgpack.GetEncoder()
	enc.Reset(w)
	enc.UseCompactInts(true)
	defer msgpack.PutEncoder(enc)

	return encodeGenericDocument(enc, d)
}

func (c AsIsCodec) Decode(encoded []byte) (types.Document, error) {
	eg := NewEncodedGenericDocument(encoded)
	eg.intAsDouble = false
	return eg, nil
}

type EncodedDocument struct {
	encoded          []byte
	fieldConstraints *FieldConstraints

	reader bytes.Reader
}

func NewEncodedDocument(fcs *FieldConstraints, data []byte) *EncodedDocument {
	e := EncodedDocument{
		fieldConstraints: fcs,
		encoded:          data,
	}

	e.reader.Reset(data)
	return &e
}

func (e *EncodedDocument) skipToExtra(dec *Decoder) error {
	l := len(e.fieldConstraints.Ordered)

	for i := 0; i < l; i++ {
		err := dec.dec.Skip()
		if err != nil {
			return err
		}
	}

	return nil
}

func (e *EncodedDocument) decodeValue(fc *FieldConstraint, dec *Decoder) (types.Value, error) {
	c, err := dec.dec.PeekCode()
	if err != nil {
		return nil, err
	}

	if fc.Type == types.DocumentValue && ((msgpcode.IsFixedArray(c)) || (c == msgpcode.Array16) || (c == msgpcode.Array32)) {
		r, err := dec.dec.DecodeRaw()
		if err != nil {
			return nil, err
		}

		return types.NewDocumentValue(NewEncodedDocument(&fc.AnonymousType.FieldConstraints, r[1:])), nil
	}

	v, err := dec.DecodeValue(fc.Type == types.AnyValue /* intAsDouble */)
	if err != nil {
		return nil, err
	}

	// ensure the returned value if of the correct type
	if fc.Type != types.AnyValue {
		return document.CastAs(v, fc.Type)
	}

	return v, nil
}

// GetByField decodes the selected field from the buffer.
func (e *EncodedDocument) GetByField(field string) (v types.Value, err error) {
	_, err = e.reader.Seek(0, io.SeekStart)
	if err != nil {
		return
	}

	// get the field from the list of field constraints
	fc, ok := e.fieldConstraints.ByField[field]
	if ok {
		dec := NewDecoder(&e.reader)
		defer dec.Close()

		// skip all fields before the selected field
		for i := 0; i < fc.Position; i++ {
			err = dec.dec.Skip()
			if err != nil {
				return nil, err
			}
		}

		return e.decodeValue(fc, dec)
	}

	// if extra field are not allowed, return an error
	if !e.fieldConstraints.AllowExtraFields {
		return nil, errors.Wrapf(types.ErrFieldNotFound, "field %q not found", field)
	}

	// otherwise, decode the field from the extra fields
	dec := NewDecoder(&e.reader)
	defer dec.Close()

	err = e.skipToExtra(dec)
	if err != nil {
		return nil, err
	}

	return NewEncodedGenericDocument(e.encoded[int(e.reader.Size())-e.reader.Len():]).GetByField(field)
}

// Iterate decodes each fields one by one and passes them to fn
// until the end of the document or until fn returns an error.
func (e *EncodedDocument) Iterate(fn func(field string, value types.Value) error) error {
	_, err := e.reader.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}

	dec := NewDecoder(&e.reader)
	defer dec.Close()

	for _, fc := range e.fieldConstraints.Ordered {
		v, err := e.decodeValue(fc, dec)
		if err != nil {
			return err
		}

		if v.Type() == types.NullValue {
			continue
		}

		err = fn(fc.Field, v)
		if err != nil {
			return err
		}
	}

	if !e.fieldConstraints.AllowExtraFields {
		return nil
	}

	return NewEncodedGenericDocument(e.encoded[int(e.reader.Size())-e.reader.Len():]).Iterate(fn)
}

func (e *EncodedDocument) MarshalJSON() ([]byte, error) {
	return document.MarshalJSON(e)
}

// bytesLen determines the size of the next string in the decoder
// based on c.
// It is originally copied from https://github.com/vmihailenco/msgpack/blob/e7759683b74a27e455669b525427cfd9aec0790e/decode_string.go#L10:19
// then adapted to our needs.
func bytesLen(c byte) (int, error) {
	if c == msgpcode.Nil {
		return -1, nil
	}

	if msgpcode.IsFixedString(c) {
		return int(c & msgpcode.FixedStrMask), nil
	}

	return 0, fmt.Errorf("msgpack: invalid code=%x decoding bytes length", c)
}

// An EncodedGenericDocument implements the types.Document
// interface on top of an encoded representation of a
// document.
// It is useful for avoiding decoding the entire document when
// only a few fields are needed.
type EncodedGenericDocument struct {
	encoded     []byte
	buf         []byte
	intAsDouble bool

	reader bytes.Reader
}

func NewEncodedGenericDocument(data []byte) *EncodedGenericDocument {
	var e EncodedGenericDocument
	e.intAsDouble = true

	e.Reset(data)
	return &e
}

func (e *EncodedGenericDocument) Reset(data []byte) {
	e.encoded = data

	e.reader.Reset(data)
}

// GetByField decodes the selected field from the buffer.
func (e *EncodedGenericDocument) GetByField(field string) (v types.Value, err error) {
	_, err = e.reader.Seek(0, io.SeekStart)
	if err != nil {
		return
	}

	dec := NewDecoder(&e.reader)
	defer dec.Close()

	if len(e.buf) == 0 {
		e.buf = make([]byte, 32)
	}

	l, err := dec.dec.DecodeMapLen()
	if err != nil {
		return
	}

	bf := []byte(field)

	var c byte
	var n int
	for i := 0; i < l; i++ {
		// this loop does basically two things:
		// - decode the field name
		// - decode the value
		// We don't use dec.dec.DecodeString() here
		// because it allocates a new string at every call
		// which is not memory efficient.
		// Since we only want to compare the field name with
		// the one received in parameter, we will decode
		// the field name ourselves and reuse the buffer
		// everytime.

		// get the type code from the decoder.
		// PeekCode doesn't move the cursor
		c, err = dec.dec.PeekCode()
		if err != nil {
			return
		}

		// Move the cursor by one to skip the type code
		err = dec.dec.ReadFull(e.buf[:1])
		if err != nil {
			return
		}

		// determine the string length
		n, err = bytesLen(c)
		if err != nil {
			return
		}

		// ensure the buffer is big enough to hold the string
		if len(e.buf) < n {
			e.buf = make([]byte, n)
		}

		// copy the field name into the buffer
		err = dec.dec.ReadFull(e.buf[:n])
		if err != nil {
			return
		}

		// if the field name is the one we are
		// looking for, decode the next value
		if bytes.Equal(e.buf[:n], bf) {
			return dec.DecodeValue(e.intAsDouble /* intAsDouble */)
		}

		// if not, we skip the next value
		err = dec.dec.Skip()
		if err != nil {
			return
		}
	}

	err = errors.Wrapf(types.ErrFieldNotFound, "field %q not found", field)
	return
}

// Iterate decodes each fields one by one and passes them to fn
// until the end of the document or until fn returns an error.
func (e *EncodedGenericDocument) Iterate(fn func(field string, value types.Value) error) error {
	_, err := e.reader.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}

	dec := NewDecoder(&e.reader)
	defer dec.Close()

	l, err := dec.dec.DecodeMapLen()
	if err != nil {
		return err
	}

	for i := 0; i < l; i++ {
		f, err := dec.dec.DecodeString()
		if err != nil {
			return err
		}

		v, err := dec.DecodeValue(e.intAsDouble /* intAsDouble */)
		if err != nil {
			return err
		}

		err = fn(f, v)
		if err != nil {
			return err
		}
	}

	return nil
}

func (e *EncodedGenericDocument) MarshalJSON() ([]byte, error) {
	return document.MarshalJSON(e)
}

// Decoder decodes Genji documents and values
// from MessagePack.
type Decoder struct {
	dec *msgpack.Decoder
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
func (d *Decoder) DecodeValue(intAsDouble bool) (v types.Value, err error) {
	c, err := d.dec.PeekCode()
	if err != nil {
		return
	}

	// decode array
	if (msgpcode.IsFixedArray(c)) || (c == msgpcode.Array16) || (c == msgpcode.Array32) {
		var a types.Array
		a, err = d.DecodeArray()
		if err != nil {
			return
		}

		v = types.NewArrayValue(a)
		return
	}

	// decode document
	if (msgpcode.IsFixedMap(c)) || (c == msgpcode.Map16) || (c == msgpcode.Map32) {
		var doc types.Document
		doc, err = d.DecodeDocument()
		if err != nil {
			return
		}

		v = types.NewDocumentValue(doc)
		return
	}

	// decode string
	if msgpcode.IsString(c) {
		var s string
		s, err = d.dec.DecodeString()
		if err != nil {
			return
		}
		v = types.NewTextValue(s)
		return
	}

	// decode fixnum (the msgpack size optimization to encode low value integers)
	// https://github.com/msgpack/msgpack/blob/master/spec.md#int-format-family
	if msgpcode.IsFixedNum(c) {
		var data int64
		data, err = d.dec.DecodeInt64()
		if err != nil {
			return
		}

		if intAsDouble {
			v = types.NewDoubleValue(float64(data))
		} else {
			v = types.NewIntegerValue(data)
		}
		return
	}

	// decode the rest
	switch c {
	case msgpcode.Nil:
		err = d.dec.DecodeNil()
		if err != nil {
			return
		}
		v = types.NewNullValue()
		return
	case msgpcode.Bin8, msgpcode.Bin16, msgpcode.Bin32:
		var data []byte
		data, err = d.dec.DecodeBytes()
		if err != nil {
			return
		}
		v = types.NewBlobValue(data)
		return
	case msgpcode.True, msgpcode.False:
		var data bool
		data, err = d.dec.DecodeBool()
		if err != nil {
			return
		}
		v = types.NewBoolValue(data)
		return
	case msgpcode.Int8, msgpcode.Int16, msgpcode.Int32, msgpcode.Int64, msgpcode.Uint8, msgpcode.Uint16, msgpcode.Uint32, msgpcode.Uint64:
		var data int64
		data, err = d.dec.DecodeInt64()
		if err != nil {
			return
		}
		if intAsDouble {
			v = types.NewDoubleValue(float64(data))
		} else {
			v = types.NewIntegerValue(data)
		}
		return
	case msgpcode.Double:
		var data float64
		data, err = d.dec.DecodeFloat64()
		if err != nil {
			return
		}
		v = types.NewDoubleValue(data)
		return
	}

	panic(fmt.Sprintf("unsupported type %v", c))
}

// DecodeDocument decodes one document from the reader.
// If the document is malformed, it will not return an error.
// However, calls to Iterate or GetByField will fail.
func (d *Decoder) DecodeDocument() (types.Document, error) {
	r, err := d.dec.DecodeRaw()
	if err != nil {
		return nil, err
	}

	return NewEncodedGenericDocument(r), nil
}

// DecodeArray decodes one array from the reader.
// If the array is malformed, this function will not return an error.
// However, calls to Iterate or GetByIndex will fail.
func (d *Decoder) DecodeArray() (types.Array, error) {
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

// An EncodedArray implements the types.Array interface on top of an
// encoded representation of an array.
// It is useful for avoiding decoding the entire array when
// only a few values are needed.
type EncodedArray []byte

// Iterate goes through all the values of the array and calls the
// given function by passing each one of them.
// If the given function returns an error, the iteration stops.
func (e EncodedArray) Iterate(fn func(i int, value types.Value) error) error {
	dec := NewDecoder(bytes.NewReader(e))
	defer dec.Close()

	l, err := dec.dec.DecodeArrayLen()
	if err != nil {
		return err
	}

	for i := 0; i < l; i++ {
		v, err := dec.DecodeValue(true /* intAsDouble */)
		if err != nil {
			return err
		}

		err = fn(i, v)
		if err != nil {
			return err
		}
	}

	return nil
}

// GetByIndex returns a value by index of the array.
func (e EncodedArray) GetByIndex(idx int) (v types.Value, err error) {
	dec := NewDecoder(bytes.NewReader(e))
	defer dec.Close()

	l, err := dec.dec.DecodeArrayLen()
	if err != nil {
		return
	}

	for i := 0; i < l; i++ {
		if i == idx {
			return dec.DecodeValue(true /* intAsDouble */)
		}

		err = dec.dec.Skip()
		if err != nil {
			return
		}
	}

	err = types.ErrValueNotFound
	return
}

func (e EncodedArray) MarshalJSON() ([]byte, error) {
	return document.MarshalJSONArray(e)
}
