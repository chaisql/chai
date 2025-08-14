package encoding

func EncodeBoolean(dst []byte, x bool) []byte {
	if x {
		return append(dst, byte(TrueValue))
	}

	return append(dst, byte(FalseValue))
}

func DecodeBoolean(b []byte) bool {
	return b[0] == byte(TrueValue) || b[0] == byte(DESC_TrueValue)
}

func EncodeNull(dst []byte) []byte {
	return append(dst, byte(NullValue))
}

// Desc changes the type of the encoded value to its descending counterpart.
// It is meant to be used in combination with one of the Encode* functions.
//
//	var buf []byte
//	buf, n = encoding.Desc(encoding.EncodeInt(buf, 10))
func Desc(dst []byte, n int) ([]byte, int) {
	if n == 0 {
		return dst, 0
	}

	dst[len(dst)-n] = 255 - dst[len(dst)-n]
	return dst, n
}
