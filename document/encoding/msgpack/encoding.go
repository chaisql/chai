package msgpack

import (
	"bytes"
	"io"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/internal/stringutil"
	"github.com/genjidb/genji/types"
	"github.com/vmihailenco/msgpack/v5"
	"github.com/vmihailenco/msgpack/v5/msgpcode"
)

// An EncodedDocument implements the types.Document
// interface on top of an encoded representation of a
// document.
// It is useful for avoiding decoding the entire document when
// only a few fields are needed.
type EncodedDocument struct {
	encoded []byte
	buf     []byte

	reader bytes.Reader
}

func NewEncodedDocument(data []byte) *EncodedDocument {
	var e EncodedDocument

	e.Reset(data)
	return &e
}

// bytesLen determines the size of the next string in the decoder
// based on c.
// It is originally copied from https://github.com/vmihailenco/msgpack/blob/e7759683b74a27e455669b525427cfd9aec0790e/decode_string.go#L10:19
// then adapted to our needs.
func bytesLen(c byte, dec *msgpack.Decoder) (int, error) {
	if c == msgpcode.Nil {
		return -1, nil
	}

	if msgpcode.IsFixedString(c) {
		return int(c & msgpcode.FixedStrMask), nil
	}

	return 0, stringutil.Errorf("msgpack: invalid code=%x decoding bytes length", c)
}

func (e *EncodedDocument) Reset(data []byte) {
	e.encoded = data

	e.reader.Reset(data)
}

// GetByField decodes the selected field from the buffer.
func (e *EncodedDocument) GetByField(field string) (v types.Value, err error) {
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
		n, err = bytesLen(c, dec.dec)
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
			return dec.DecodeValue()
		}

		// if not, we skip the next value
		err = dec.dec.Skip()
		if err != nil {
			return
		}
	}

	err = document.ErrFieldNotFound
	return
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

	l, err := dec.dec.DecodeMapLen()
	if err != nil {
		return err
	}

	for i := 0; i < l; i++ {
		f, err := dec.dec.DecodeString()
		if err != nil {
			return err
		}

		v, err := dec.DecodeValue()
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
		v, err := dec.DecodeValue()
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
			return dec.DecodeValue()
		}

		err = dec.dec.Skip()
		if err != nil {
			return
		}
	}

	err = document.ErrValueNotFound
	return
}
