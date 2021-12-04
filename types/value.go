package types

import (
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"math"
	"strconv"
	"strings"

	"github.com/genjidb/genji/internal/errors"
	"github.com/genjidb/genji/internal/stringutil"
)

// A Value stores encoded data alongside its type.
type value struct {
	tp ValueType
	v  interface{}
}

var _ Value = &value{}

// NewNullValue returns a Null value.
func NewNullValue() Value {
	return &value{
		tp: NullValue,
	}
}

// NewBoolValue encodes x and returns a value.
func NewBoolValue(x bool) Value {
	return &value{
		tp: BoolValue,
		v:  x,
	}
}

// NewIntegerValue encodes x and returns a value whose type depends on the
// magnitude of x.
func NewIntegerValue(x int64) Value {
	return &value{
		tp: IntegerValue,
		v:  int64(x),
	}
}

// NewDoubleValue encodes x and returns a value.
func NewDoubleValue(x float64) Value {
	return &value{
		tp: DoubleValue,
		v:  x,
	}
}

// NewBlobValue encodes x and returns a value.
func NewBlobValue(x []byte) Value {
	return &value{
		tp: BlobValue,
		v:  x,
	}
}

// NewTextValue encodes x and returns a value.
func NewTextValue(x string) Value {
	return &value{
		tp: TextValue,
		v:  x,
	}
}

// NewArrayValue returns a value of type Array.
func NewArrayValue(a Array) Value {
	return &value{
		tp: ArrayValue,
		v:  a,
	}
}

// NewDocumentValue returns a value of type Document.
func NewDocumentValue(d Document) Value {
	return &value{
		tp: DocumentValue,
		v:  d,
	}
}

// NewValueWith creates a value with the given type and value.
func NewValueWith(t ValueType, v interface{}) Value {
	return &value{
		tp: t,
		v:  v,
	}
}

func (v *value) V() interface{} {
	return v.v
}

func (v *value) Type() ValueType {
	return v.tp
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
	case BoolValue:
		return v.V() == false, nil
	case IntegerValue:
		return v.V() == int64(0), nil
	case DoubleValue:
		return v.V() == float64(0), nil
	case BlobValue:
		return v.V() == nil, nil
	case TextValue:
		return v.V() == "", nil
	case ArrayValue:
		// The zero value of an array is an empty array.
		// Thus, if GetByIndex(0) returns the ErrValueNotFound
		// it means that the array is empty.
		_, err := v.V().(Array).GetByIndex(0)
		if errors.Is(err, ErrValueNotFound) {
			return true, nil
		}
		return false, err
	case DocumentValue:
		err := v.V().(Document).Iterate(func(_ string, _ Value) error {
			// We return an error in the first iteration to stop it.
			return errors.Wrap(errStop)
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

func (v *value) String() string {
	data, _ := v.MarshalText()
	return string(data)
}

func (v *value) MarshalText() ([]byte, error) {
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
	case BoolValue:
		dst.WriteString(strconv.FormatBool(v.V().(bool)))
		return nil
	case IntegerValue:
		dst.WriteString(strconv.FormatInt(v.V().(int64), 10))
		return nil
	case DoubleValue:
		f := v.V().(float64)
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
		dst.WriteString(strconv.FormatFloat(v.V().(float64), fmt, prec, 64))
		return nil
	case TextValue:
		dst.WriteString(strconv.Quote(v.V().(string)))
		return nil
	case BlobValue:
		src := v.V().([]byte)
		dst.WriteString("\"\\x")
		hex.NewEncoder(dst).Write(src)
		dst.WriteByte('"')
		return nil
	case ArrayValue:
		var nonempty bool
		dst.WriteByte('[')
		err := v.V().(Array).Iterate(func(i int, value Value) error {
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
		err := v.V().(Document).Iterate(func(field string, value Value) error {
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
		return stringutil.Errorf("unexpected type: %d", v.Type())
	}
}

func newline(dst *bytes.Buffer, prefix, indent string, depth int) {
	dst.WriteString(prefix)
	for i := 0; i < depth; i++ {
		dst.WriteString(indent)
	}
}

// MarshalJSON implements the json.Marshaler interface.
func (v *value) MarshalJSON() ([]byte, error) {
	switch v.Type() {
	case BoolValue, IntegerValue, TextValue:
		return v.MarshalText()
	case NullValue:
		return []byte("null"), nil
	case DoubleValue:
		f := v.V().(float64)
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
		return strconv.AppendFloat(nil, v.V().(float64), fmt, prec, 64), nil
	case BlobValue:
		src := v.V().([]byte)
		dst := make([]byte, base64.StdEncoding.EncodedLen(len(src))+2)
		dst[0] = '"'
		dst[len(dst)-1] = '"'
		base64.StdEncoding.Encode(dst[1:], src)
		return dst, nil
	case ArrayValue:
		return jsonArray{Array: v.V().(Array)}.MarshalJSON()
	case DocumentValue:
		return jsonDocument{Document: v.V().(Document)}.MarshalJSON()
	default:
		return nil, stringutil.Errorf("unexpected type: %d", v.Type())
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
