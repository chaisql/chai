package encoding

import (
	"encoding/binary"
	"unsafe"
)

func EncodeBlob(dst []byte, x []byte) []byte {
	// encode the length as a varint
	buf := make([]byte, binary.MaxVarintLen64+1)
	buf[0] = BlobValue
	n := binary.PutUvarint(buf[1:], uint64(len(x)))

	dst = append(dst, buf[:n+1]...)
	return append(dst, x...)
}

func DecodeBlob(b []byte) ([]byte, int) {
	// skip type
	b = b[1:]
	// decode the length as a varint
	l, n := binary.Uvarint(b)
	return b[n : n+int(l)], 1 + n + int(l)
}

func EncodeText(dst []byte, x string) []byte {
	// encode the length as a varint
	buf := make([]byte, binary.MaxVarintLen64+1)
	buf[0] = TextValue
	n := binary.PutUvarint(buf[1:], uint64(len(x)))

	dst = append(dst, buf[:n+1]...)
	return append(dst, x...)
}

func DecodeText(b []byte) (string, int) {
	// skip type
	b = b[1:]
	// decode the length as a varint
	l, n := binary.Uvarint(b)
	b = b[n : n+int(l)]
	return *(*string)(unsafe.Pointer(&b)), 1 + n + int(l)
}
