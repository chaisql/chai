package row

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/chaisql/chai/internal/stringutil"
	"github.com/chaisql/chai/internal/types"
)

// MarshalJSON encodes a row to json.
func MarshalJSON(r Row) ([]byte, error) {
	var buf bytes.Buffer

	buf.WriteByte('{')

	var notFirst bool
	err := r.Iterate(func(c string, v types.Value) error {
		if notFirst {
			buf.WriteString(", ")
		}
		notFirst = true

		buf.WriteString(strconv.Quote(c))
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

func MarshalTextIndent(r Row, prefix, indent string) ([]byte, error) {
	var buf bytes.Buffer
	buf.WriteByte('{')
	var i int
	err := r.Iterate(func(field string, value types.Value) error {
		if i > 0 {
			buf.WriteByte(',')
			if prefix == "" {
				buf.WriteByte(' ')
			}
		}
		newline(&buf, prefix, indent, 1)
		i++

		var ident string
		if strings.HasPrefix(field, "\"") {
			ident = stringutil.NormalizeIdentifier(field, '`')
		} else {
			ident = stringutil.NormalizeIdentifier(field, '"')
		}
		buf.WriteString(ident)
		buf.WriteString(": ")

		return marshalText(&buf, value)
	})
	if err != nil {
		return nil, err
	}
	newline(&buf, prefix, indent, 0)
	buf.WriteRune('}')
	return buf.Bytes(), nil
}

func marshalText(dst *bytes.Buffer, v types.Value) error {
	if v.V() == nil {
		dst.WriteString("NULL")
		return nil
	}

	switch v.Type() {
	case types.TypeNull:
		dst.WriteString("NULL")
		return nil
	case types.TypeBoolean:
		dst.WriteString(strconv.FormatBool(types.AsBool(v)))
		return nil
	case types.TypeInteger, types.TypeBigint:
		dst.WriteString(strconv.FormatInt(types.AsInt64(v), 10))
		return nil
	case types.TypeDouble:
		f := types.AsFloat64(v)
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
		dst.WriteString(strconv.FormatFloat(types.AsFloat64(v), fmt, prec, 64))
		return nil
	case types.TypeTimestamp:
		dst.WriteString(strconv.Quote(types.AsTime(v).Format(time.RFC3339Nano)))
		return nil
	case types.TypeText:
		dst.WriteString(strconv.Quote(types.AsString(v)))
		return nil
	case types.TypeBlob:
		src := types.AsByteSlice(v)
		dst.WriteString("\"\\x")
		_, _ = hex.NewEncoder(dst).Write(src)
		dst.WriteByte('"')
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
