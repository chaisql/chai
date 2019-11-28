package record

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sort"

	"github.com/asdine/genji/value"
)

// Format is an encoding format used to encode and decode records.
// It is composed of a header and a body.
// The header defines a list of fields, offsets and relevant metadata.
// The body contains each fields data one concatenated one after another.
type Format struct {
	Header Header
	Body   []byte
}

// Decode the given data into the format.
func (f *Format) Decode(data []byte) error {
	n, err := f.Header.Decode(data)
	if err != nil {
		return err
	}

	f.Body = data[n:]
	return nil
}

// A Header contains a representation of a record's metadata.
type Header struct {
	// Size of the header
	Size uint64
	// Number of fields
	FieldsCount uint64
	// List of headers for all the fields.
	FieldHeaders []FieldHeader
}

// Decode data into the header.
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

// BodySize returns the size of the body.
func (h *Header) BodySize() int {
	var size uint64

	for _, fh := range h.FieldHeaders {
		size += fh.Size
	}

	return int(size)
}

// WriteTo encodes the header into w.
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

// FieldHeader represents the metadata of a field.
type FieldHeader struct {
	// Size of the name of the field
	NameSize uint64
	// Name of the field
	Name []byte
	// Type of the field, corresponds to the value.Type
	Type uint64
	// Size of the data of the field
	Size uint64
	// Offset describing where the field is located, starting
	// from the end of the format header.
	Offset uint64

	nameString string // used for encoding
	buf        [binary.MaxVarintLen64]byte
}

// Decode the data into the field header.
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

// WriteTo encodes the field header into w.
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

// Encode takes a record and encodes it using the Format.
func Encode(r Record) ([]byte, error) {
	var format Format

	var offset uint64
	var dataList [][]byte

	// copy the record into a buffer and sort the record
	// by field names
	switch t := r.(type) {
	case FieldBuffer:
		fb := &t
		sort.Sort(fb)
		r = fb
	case *FieldBuffer:
		sort.Sort(t)
	default:
		var fb FieldBuffer
		err := fb.ScanRecord(r)
		if err != nil {
			return nil, err
		}
		sort.Sort(&fb)
		r = &fb
	}

	err := r.Iterate(func(f Field) error {
		if f.Type == value.Object {
			var err error
			f.Data, err = Encode(f.nestedRecord)
			if err != nil {
				return err
			}
		}

		format.Header.FieldHeaders = append(format.Header.FieldHeaders, FieldHeader{
			NameSize:   uint64(len(f.Name)),
			nameString: f.Name,
			Type:       uint64(f.Type),
			Size:       uint64(len(f.Data)),
			Offset:     offset,
		})

		offset += uint64(len(f.Data))
		dataList = append(dataList, f.Data)
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

// DecodeField reads a single field from data without decoding the entire data.
func DecodeField(data []byte, fieldName string) (Field, error) {
	hsize, n := binary.Uvarint(data)
	if n <= 0 {
		return Field{}, errors.New("can't decode data")
	}

	hdata := data[n : n+int(hsize)]
	body := data[n+len(hdata):]

	// skip number of fields
	_, n = binary.Uvarint(hdata)
	if n <= 0 {
		return Field{}, errors.New("can't decode data")
	}
	hdata = hdata[n:]

	var fh FieldHeader
	for len(hdata) > 0 {
		n, err := fh.Decode(hdata)
		if err != nil {
			return Field{}, err
		}
		hdata = hdata[n:]

		if fieldName == string(fh.Name) {
			data := body[fh.Offset : fh.Offset+fh.Size]

			f := Field{
				Name: fieldName,
				Value: value.Value{
					Type: value.Type(fh.Type),
				},
			}

			if f.Type == value.Object {
				f.nestedRecord = EncodedRecord(data)
			} else if len(data) > 0 {
				// make sure f.Data == nil to ease comparisons
				f.Data = body[fh.Offset : fh.Offset+fh.Size]
			}
			return f, nil
		}
	}

	return Field{}, fmt.Errorf("field %s not found", fieldName)
}

// An EncodedRecord implements the record interface on top of an encoded representation of a
// record.
// It is useful to avoid decoding the entire record when only a few fields are needed.
type EncodedRecord []byte

// GetField decodes the selected field.
func (e EncodedRecord) GetField(name string) (Field, error) {
	return DecodeField(e, name)
}

// Iterate decodes each fields one by one and passes them to fn until the end of the record
// or until fn returns an error.
func (e EncodedRecord) Iterate(fn func(Field) error) error {
	var format Format
	err := format.Decode(e)
	if err != nil {
		return err
	}

	for _, fh := range format.Header.FieldHeaders {
		data := format.Body[fh.Offset : fh.Offset+fh.Size]
		f := Field{
			Name: string(fh.Name),
			Value: value.Value{
				Type: value.Type(fh.Type),
			},
		}

		if f.Type == value.Object {
			f.nestedRecord = EncodedRecord(data)
		} else {
			f.Data = data
		}
		err = fn(f)
		if err != nil {
			return err
		}
	}

	return nil
}
