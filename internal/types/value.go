package types

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/chaisql/chai/internal/stringutil"
)

func AsBool(v Value) bool {
	bv, ok := v.(BooleanValue)
	if !ok {
		return v.V().(bool)
	}

	return bool(bv)
}

func AsInt64(v Value) int64 {
	iv, ok := v.(IntegerValue)
	if !ok {
		return v.V().(int64)
	}

	return int64(iv)
}

func AsFloat64(v Value) float64 {
	dv, ok := v.(DoubleValue)
	if !ok {
		return v.V().(float64)
	}

	return float64(dv)
}

func AsTime(v Value) time.Time {
	tv, ok := v.(TimestampValue)
	if !ok {
		return v.V().(time.Time)
	}

	return time.Time(tv)
}

func AsString(v Value) string {
	tv, ok := v.(TextValue)
	if !ok {
		return v.V().(string)
	}

	return string(tv)
}

func AsByteSlice(v Value) []byte {
	bv, ok := v.(BlobValue)
	if !ok {
		return v.V().([]byte)
	}

	return bv
}

func AsArray(v Value) Array {
	av, ok := v.(*ArrayValue)
	if !ok {
		return v.V().(Array)
	}

	return av.a
}

func AsObject(v Value) Object {
	ov, ok := v.(*ObjectValue)
	if !ok {
		return v.V().(Object)
	}

	return ov.o
}

func Is[T any](v Value) (T, bool) {
	x, ok := v.V().(T)
	return x, ok
}

func IsNull(v Value) bool {
	return v == nil || v.Type() == TypeNull
}

// IsTruthy returns whether v is not Equal to the zero value of its type.
func IsTruthy(v Value) (bool, error) {
	if v.Type() == TypeNull {
		return false, nil
	}

	b, err := v.IsZero()
	return !b, err
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
	case TypeNull:
		dst.WriteString("NULL")
		return nil
	case TypeBoolean:
		dst.WriteString(strconv.FormatBool(AsBool(v)))
		return nil
	case TypeInteger:
		dst.WriteString(strconv.FormatInt(AsInt64(v), 10))
		return nil
	case TypeDouble:
		f := AsFloat64(v)
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
		dst.WriteString(strconv.FormatFloat(AsFloat64(v), fmt, prec, 64))
		return nil
	case TypeTimestamp:
		dst.WriteString(strconv.Quote(AsTime(v).Format(time.RFC3339Nano)))
		return nil
	case TypeText:
		dst.WriteString(strconv.Quote(AsString(v)))
		return nil
	case TypeBlob:
		src := AsByteSlice(v)
		dst.WriteString("\"\\x")
		hex.NewEncoder(dst).Write(src)
		dst.WriteByte('"')
		return nil
	case TypeArray:
		var nonempty bool
		dst.WriteByte('[')
		err := AsArray(v).Iterate(func(i int, value Value) error {
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
	case TypeObject:
		dst.WriteByte('{')
		var i int
		err := AsObject(v).Iterate(func(field string, value Value) error {
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

type jsonObject struct {
	Object
}

func (j jsonObject) MarshalJSON() ([]byte, error) {
	var buf bytes.Buffer

	buf.WriteByte('{')

	var notFirst bool
	err := j.Object.Iterate(func(f string, v Value) error {
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
