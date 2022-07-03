package types

import (
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/genjidb/genji/internal/stringutil"
)

// A Value stores encoded data alongside its type.
type value[T any] struct {
	tp ValueType
	v  T
}

var _ Value = &value[bool]{}

// NewNullValue returns a Null value.
func NewNullValue() Value {
	return &value[struct{}]{
		tp: NullValue,
	}
}

// NewBoolValue encodes x and returns a value.
func NewBoolValue(x bool) Value {
	return &value[bool]{
		tp: BooleanValue,
		v:  x,
	}
}

// NewIntegerValue encodes x and returns a value whose type depends on the
// magnitude of x.
func NewIntegerValue(x int64) Value {
	return &value[int64]{
		tp: IntegerValue,
		v:  x,
	}
}

// NewDoubleValue encodes x and returns a value.
func NewDoubleValue(x float64) Value {
	return &value[float64]{
		tp: DoubleValue,
		v:  x,
	}
}

// NewBlobValue encodes x and returns a value.
func NewBlobValue(x []byte) Value {
	return &value[[]byte]{
		tp: BlobValue,
		v:  x,
	}
}

// NewTextValue encodes x and returns a value.
func NewTextValue(x string) Value {
	return &value[string]{
		tp: TextValue,
		v:  x,
	}
}

// NewArrayValue returns a value of type Array.
func NewArrayValue(a Array) Value {
	return &value[Array]{
		tp: ArrayValue,
		v:  a,
	}
}

// NewDocumentValue returns a value of type Document.
func NewDocumentValue(d Document) Value {
	return &value[Document]{
		tp: DocumentValue,
		v:  d,
	}
}

// NewValueWith creates a value with the given type and value.
func NewValueWith[T any](t ValueType, v T) Value {
	return &value[T]{
		tp: t,
		v:  v,
	}
}

func (v *value[T]) V() any {
	if v.tp == NullValue {
		return nil
	}

	return v.v
}

func (v *value[T]) Type() ValueType {
	return v.tp
}

func As[T any](v Value) T {
	vv, ok := v.(*value[T])
	if !ok {
		return v.V().(T)
	}

	return vv.v
}

func Is[T any](v Value) (T, bool) {
	vv, ok := v.(*value[T])
	if !ok {
		x, ok := v.V().(T)
		return x, ok
	}

	return vv.v, true
}

func IsNull(v Value) bool {
	return v == nil || v.Type() == NullValue
}

// IsTruthy returns whether v is not equal to the zero value of its type.
func IsTruthy(v Value) (bool, error) {
	if v.Type() == NullValue {
		return false, nil
	}

	b, err := IsZeroValue(v)
	return !b, err
}

// IsZeroValue indicates if the value data is the zero value for the value type.
// This function doesn't perform any allocation.
func IsZeroValue(v Value) (bool, error) {
	switch v.Type() {
	case BooleanValue:
		return !As[bool](v), nil
	case IntegerValue:
		return As[int64](v) == int64(0), nil
	case DoubleValue:
		return As[float64](v) == float64(0), nil
	case BlobValue:
		return As[[]byte](v) == nil, nil
	case TextValue:
		return As[string](v) == "", nil
	case ArrayValue:
		// The zero value of an array is an empty array.
		// Thus, if GetByIndex(0) returns the ErrValueNotFound
		// it means that the array is empty.
		_, err := As[Array](v).GetByIndex(0)
		if errors.Is(err, ErrValueNotFound) {
			return true, nil
		}
		return false, err
	case DocumentValue:
		err := As[Document](v).Iterate(func(_ string, _ Value) error {
			// We return an error in the first iteration to stop it.
			return errors.WithStack(errStop)
		})
		if err == nil {
			// If err is nil, it means that we didn't iterate,
			// thus the document is empty.
			return true, nil
		}
		if errors.Is(err, errStop) {
			// If err is errStop, it means that we iterate
			// at least once, thus the document is not empty.
			return false, nil
		}
		// An unexpecting error occurs, let's return it!
		return false, err
	}

	return false, nil
}

func (v *value[T]) String() string {
	data, _ := v.MarshalText()
	return string(data)
}

func (v *value[T]) MarshalText() ([]byte, error) {
	return MarshalTextIndent(v, "", "")
}

func MarshalTextIndent(v Value, prefix, indent string) ([]byte, error) {
	var buf bytes.Buffer

	err := marshalText(&buf, v, prefix, indent, 0)
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func marshalText(dst *bytes.Buffer, v Value, prefix, indent string, depth int) error {
	if v.V() == nil {
		dst.WriteString("NULL")
		return nil
	}

	switch v.Type() {
	case NullValue:
		dst.WriteString("NULL")
		return nil
	case BooleanValue:
		dst.WriteString(strconv.FormatBool(As[bool](v)))
		return nil
	case IntegerValue:
		dst.WriteString(strconv.FormatInt(As[int64](v), 10))
		return nil
	case DoubleValue:
		f := As[float64](v)
		abs := math.Abs(f)
		fmt := byte('f')
		if abs != 0 {
			if abs < 1e-6 || abs >= 1e15 {
				fmt = 'e'
			}
		}

		// By default the precision is -1 to use the smallest number of digits.
		// See https://pkg.go.dev/strconv#FormatFloat
		prec := -1
		// if the number is round, add .0
		if float64(int64(f)) == f {
			prec = 1
		}
		dst.WriteString(strconv.FormatFloat(As[float64](v), fmt, prec, 64))
		return nil
	case TextValue:
		dst.WriteString(strconv.Quote(As[string](v)))
		return nil
	case BlobValue:
		src := As[[]byte](v)
		dst.WriteString("\"\\x")
		hex.NewEncoder(dst).Write(src)
		dst.WriteByte('"')
		return nil
	case ArrayValue:
		var nonempty bool
		dst.WriteByte('[')
		err := As[Array](v).Iterate(func(i int, value Value) error {
			nonempty = true
			if i > 0 {
				dst.WriteByte(',')
				if prefix == "" {
					dst.WriteByte(' ')
				}
			}
			newline(dst, prefix, indent, depth+1)

			return marshalText(dst, value, prefix, indent, depth+1)
		})
		if err != nil {
			return err
		}
		if nonempty && prefix != "" {
			newline(dst, prefix, indent, depth)
		}
		dst.WriteByte(']')
		return nil
	case DocumentValue:
		dst.WriteByte('{')
		var i int
		err := As[Document](v).Iterate(func(field string, value Value) error {
			if i > 0 {
				dst.WriteByte(',')
				if prefix == "" {
					dst.WriteByte(' ')
				}
			}
			newline(dst, prefix, indent, depth+1)
			i++

			var ident string
			if strings.HasPrefix(field, "\"") {
				ident = stringutil.NormalizeIdentifier(field, '`')
			} else {
				ident = stringutil.NormalizeIdentifier(field, '"')
			}
			dst.WriteString(ident)
			dst.WriteString(": ")

			return marshalText(dst, value, prefix, indent, depth+1)
		})
		if err != nil {
			return err
		}
		newline(dst, prefix, indent, depth)
		dst.WriteRune('}')
		return nil
	default:
		return fmt.Errorf("unexpected type: %d", v.Type())
	}
}

func newline(dst *bytes.Buffer, prefix, indent string, depth int) {
	dst.WriteString(prefix)
	for i := 0; i < depth; i++ {
		dst.WriteString(indent)
	}
}

// MarshalJSON implements the json.Marshaler interface.
func (v *value[T]) MarshalJSON() ([]byte, error) {
	switch v.Type() {
	case BooleanValue, IntegerValue, TextValue:
		return v.MarshalText()
	case NullValue:
		return []byte("null"), nil
	case DoubleValue:
		f := As[float64](v)
		abs := math.Abs(f)
		fmt := byte('f')
		if abs != 0 {
			if abs < 1e-6 || abs >= 1e15 {
				fmt = 'e'
			}
		}

		// By default the precision is -1 to use the smallest number of digits.
		// See https://pkg.go.dev/strconv#FormatFloat
		prec := -1
		return strconv.AppendFloat(nil, As[float64](v), fmt, prec, 64), nil
	case BlobValue:
		src := As[[]byte](v)
		dst := make([]byte, base64.StdEncoding.EncodedLen(len(src))+2)
		dst[0] = '"'
		dst[len(dst)-1] = '"'
		base64.StdEncoding.Encode(dst[1:], src)
		return dst, nil
	case ArrayValue:
		return jsonArray{Array: As[Array](v)}.MarshalJSON()
	case DocumentValue:
		return jsonDocument{Document: As[Document](v)}.MarshalJSON()
	default:
		return nil, fmt.Errorf("unexpected type: %d", v.Type())
	}
}

type jsonArray struct {
	Array
}

func (j jsonArray) MarshalJSON() ([]byte, error) {
	var buf bytes.Buffer

	buf.WriteRune('[')
	err := j.Array.Iterate(func(i int, v Value) error {
		if i > 0 {
			buf.WriteString(", ")
		}

		data, err := v.MarshalJSON()
		if err != nil {
			return err
		}

		_, err = buf.Write(data)
		return err
	})
	if err != nil {
		return nil, err
	}
	buf.WriteRune(']')

	return buf.Bytes(), nil
}

type jsonDocument struct {
	Document
}

func (j jsonDocument) MarshalJSON() ([]byte, error) {
	var buf bytes.Buffer

	buf.WriteByte('{')

	var notFirst bool
	err := j.Document.Iterate(func(f string, v Value) error {
		if notFirst {
			buf.WriteString(", ")
		}
		notFirst = true

		buf.WriteString(strconv.Quote(f))
		buf.WriteString(": ")

		data, err := v.MarshalJSON()
		if err != nil {
			return err
		}
		_, err = buf.Write(data)
		return err
	})
	if err != nil {
		return nil, err
	}

	buf.WriteByte('}')

	return buf.Bytes(), nil
}
