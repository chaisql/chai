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

func (f *Format) Decode(data []byte) error {
	n, err := f.Header.Decode(data)
	if err != nil {
		return err
	}

	f.Body = data[n:]
	return nil
}

type Header struct {
	Size         uint64
	FieldsCount  uint64
	FieldHeaders []FieldHeader
}

func (h *Header) Decode(data []byte) (int, error) {
	var n int

	h.Size, n = binary.Uvarint(data)
	if n <= 0 {
		return 0, errors.New("can't decode data")
	}

	hdata := data[n : n+int(h.Size)]
	read := n + int(h.Size)

	h.FieldsCount, n = binary.Uvarint(hdata)
	if n <= 0 {
		return 0, errors.New("can't decode data")
	}
	hdata = hdata[n:]

	h.FieldHeaders = make([]FieldHeader, 0, int(h.FieldsCount))
	for len(hdata) > 0 {
		var fh FieldHeader
		n, err := fh.Decode(hdata)
		if err != nil {
			return 0, err
		}
		hdata = hdata[n:]

		h.FieldHeaders = append(h.FieldHeaders, fh)
	}

	return read, nil
}

func (h *Header) BodySize() int {
	var size uint64

	for _, fh := range h.FieldHeaders {
		size += fh.Size
	}

	return int(size)
}

func (h *Header) WriteTo(w io.Writer) (int64, error) {
	intBuf := make([]byte, binary.MaxVarintLen64)
	var buf bytes.Buffer

	// number of fields
	h.FieldsCount = uint64(len(h.FieldHeaders))
	n := binary.PutUvarint(intBuf, h.FieldsCount)
	_, err := buf.Write(intBuf[:n])
	if err != nil {
		return 0, err
	}

	for _, fh := range h.FieldHeaders {
		_, err := fh.WriteTo(&buf)
		if err != nil {
			return 0, err
		}
	}

	// header size
	h.Size = uint64(buf.Len())
	n = binary.PutUvarint(intBuf, h.Size)
	_, err = w.Write(intBuf[:n])
	if err != nil {
		return 0, err
	}

	return buf.WriteTo(w)
}

type FieldHeader struct {
	NameSize uint64
	Name     []byte
	Type     uint64
	Size     uint64
	Offset   uint64

	nameString string // used for encoding
	buf        [binary.MaxVarintLen64]byte
}

func (f *FieldHeader) Decode(data []byte) (int, error) {
	var n, read int

	// name size
	f.NameSize, n = binary.Uvarint(data)
	if n <= 0 {
		return 0, errors.New("can't decode data")
	}
	data = data[n:]
	read += n

	// name
	f.Name = data[:f.NameSize]
	data = data[f.NameSize:]
	read += int(f.NameSize)

	// type
	f.Type, n = binary.Uvarint(data)
	if n <= 0 {
		return 0, errors.New("can't decode data")
	}
	data = data[n:]
	read += n

	// size
	f.Size, n = binary.Uvarint(data)
	if n <= 0 {
		return 0, errors.New("can't decode data")
	}
	data = data[n:]
	read += n

	// offset
	f.Offset, n = binary.Uvarint(data)
	if n <= 0 {
		return 0, errors.New("can't decode data")
	}
	data = data[n:]
	read += n

	return read, nil
}

func (f *FieldHeader) WriteTo(w io.Writer) (int64, error) {
	var written int

	// name size
	n := binary.PutUvarint(f.buf[:], f.NameSize)
	_, err := w.Write(f.buf[:n])
	if err != nil {
		return 0, err
	}
	written += n

	// name
	if buf, ok := w.(*bytes.Buffer); ok && f.nameString != "" {
		n, err = buf.WriteString(f.nameString)
	} else {
		n, err = w.Write(f.Name)
	}
	if err != nil {
		return 0, err
	}
	written += n

	// type
	n = binary.PutUvarint(f.buf[:], f.Type)
	_, err = w.Write(f.buf[:n])
	if err != nil {
		return 0, err
	}
	written += n

	// size
	n = binary.PutUvarint(f.buf[:], f.Size)
	_, err = w.Write(f.buf[:n])
	if err != nil {
		return 0, err
	}
	written += n

	// offset
	n = binary.PutUvarint(f.buf[:], f.Offset)
	_, err = w.Write(f.buf[:n])
	if err != nil {
		return 0, err
	}
	written += n

	return int64(written), nil
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
			NameSize:   uint64(len(f.Name)),
			nameString: f.Name,
			Type:       uint64(f.Type),
			Size:       uint64(len(f.Data)),
			Offset:     offset,
		})

		offset += uint64(len(f.Data))
	}

	var buf bytes.Buffer
	_, err := format.Header.WriteTo(&buf)
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

func DecodeField(data []byte, fieldName string) (field.Field, error) {
	hsize, n := binary.Uvarint(data)
	if n <= 0 {
		return field.Field{}, errors.New("can't decode data")
	}

	hdata := data[n : n+int(hsize)]
	body := data[n+len(hdata):]

	// skip number of fields
	_, n = binary.Uvarint(hdata)
	if n <= 0 {
		return field.Field{}, errors.New("can't decode data")
	}
	hdata = hdata[n:]

	var fh FieldHeader
	for len(hdata) > 0 {
		n, err := fh.Decode(hdata)
		if err != nil {
			return field.Field{}, err
		}
		hdata = hdata[n:]

		if fieldName == string(fh.Name) {
			return field.Field{
				Name: fieldName,
				Type: field.Type(fh.Type),
				Data: body[fh.Offset : fh.Offset+fh.Size],
			}, nil
		}
	}

	return field.Field{}, errors.New("not found")
}
