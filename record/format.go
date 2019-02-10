package record

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"

	"github.com/asdine/genji/field"
)

type Format struct {
	Header Header
	Body   []byte
}

type Header struct {
	Size         uint64
	FieldHeaders []FieldHeader
}

func (h *Header) BodySize() int {
	var size uint64

	for _, fh := range h.FieldHeaders {
		size += fh.Size
	}

	return int(size)
}

func (h *Header) WriteTo(w io.Writer) error {
	intBuf := make([]byte, binary.MaxVarintLen64)
	var buf bytes.Buffer

	for _, fh := range h.FieldHeaders {
		// name size
		n := binary.PutUvarint(intBuf, fh.NameSize)
		_, err := buf.Write(intBuf[:n])
		if err != nil {
			return err
		}

		// name
		n, err = buf.WriteString(fh.Name)
		if err != nil {
			return err
		}

		// type
		n = binary.PutUvarint(intBuf, fh.Type)
		_, err = buf.Write(intBuf[:n])
		if err != nil {
			return err
		}

		// size
		n = binary.PutUvarint(intBuf, fh.Size)
		_, err = buf.Write(intBuf[:n])
		if err != nil {
			return err
		}

		// offset
		n = binary.PutUvarint(intBuf, fh.Offset)
		_, err = buf.Write(intBuf[:n])
		if err != nil {
			return err
		}
	}

	// header size
	h.Size = uint64(buf.Len())
	n := binary.PutUvarint(intBuf, h.Size)

	_, err := w.Write(intBuf[:n])
	if err != nil {
		return err
	}

	_, err = buf.WriteTo(w)

	return err
}

type FieldHeader struct {
	NameSize uint64
	Name     string
	Type     uint64
	Size     uint64
	Offset   uint64
}

func Encode(r Record) ([]byte, error) {
	var format Format

	var offset uint64
	c := r.Cursor()
	for c.Next() {
		if err := c.Err(); err != nil {
			return nil, err
		}

		f := c.Field()
		format.Header.FieldHeaders = append(format.Header.FieldHeaders, FieldHeader{
			NameSize: uint64(len(f.Name)),
			Name:     f.Name,
			Type:     uint64(f.Type),
			Size:     uint64(len(f.Data)),
			Offset:   offset,
		})

		offset += uint64(len(f.Data))
	}

	var buf bytes.Buffer
	err := format.Header.WriteTo(&buf)
	if err != nil {
		return nil, err
	}

	buf.Grow(format.Header.BodySize())

	c = r.Cursor()
	for c.Next() {
		if err := c.Err(); err != nil {
			return nil, err
		}

		f := c.Field()
		_, err = buf.Write(f.Data)
		if err != nil {
			return nil, err
		}
	}

	return buf.Bytes(), nil
}

func DecodeField(data []byte, fieldName string) (*field.Field, error) {
	hsize, n := binary.Uvarint(data)
	if n <= 0 {
		return nil, errors.New("can't decode data")
	}

	hdata := data[n : n+int(hsize)]
	body := data[n+len(hdata):]

	for len(hdata) > 0 {
		// name size
		nameSize, n := binary.Uvarint(hdata)
		if n <= 0 {
			return nil, errors.New("can't decode data")
		}
		hdata = hdata[n:]

		// name
		name := hdata[:nameSize]
		hdata = hdata[nameSize:]

		// type
		typ, n := binary.Uvarint(hdata)
		if n <= 0 {
			return nil, errors.New("can't decode data")
		}
		hdata = hdata[n:]

		// size
		size, n := binary.Uvarint(hdata)
		if n <= 0 {
			return nil, errors.New("can't decode data")
		}
		hdata = hdata[n:]

		// offset
		offset, n := binary.Uvarint(hdata)
		if n <= 0 {
			return nil, errors.New("can't decode data")
		}
		hdata = hdata[n:]

		if fieldName == string(name) {
			return &field.Field{
				Name: fieldName,
				Type: field.Type(typ),
				Data: body[offset : offset+size],
			}, nil
		}
	}

	return nil, errors.New("not found")
}
