package document

import (
	"bytes"
	"encoding/base64"
	"math"
	"strconv"
	"strings"

	"github.com/buger/jsonparser"
	"github.com/genjidb/genji/internal/stringutil"
	"github.com/genjidb/genji/types"
)

func parseJSONValue(dataType jsonparser.ValueType, data []byte) (v types.Value, err error) {
	switch dataType {
	case jsonparser.Null:
		return types.NewNullValue(), nil
	case jsonparser.Boolean:
		b, err := jsonparser.ParseBoolean(data)
		if err != nil {
			return nil, err
		}
		return types.NewBoolValue(b), nil
	case jsonparser.Number:
		i, err := jsonparser.ParseInt(data)
		if err != nil {
			// if it's too big to fit in an int64, let's try parsing this as a floating point number
			f, err := jsonparser.ParseFloat(data)
			if err != nil {
				return nil, err
			}

			return types.NewDoubleValue(f), nil
		}

		return types.NewIntegerValue(i), nil
	case jsonparser.String:
		s, err := jsonparser.ParseString(data)
		if err != nil {
			return nil, err
		}
		return types.NewTextValue(s), nil
	case jsonparser.Array:
		buf := NewValueBuffer()
		err := buf.UnmarshalJSON(data)
		if err != nil {
			return nil, err
		}

		return types.NewArrayValue(buf), nil
	case jsonparser.Object:
		buf := NewFieldBuffer()
		err = buf.UnmarshalJSON(data)
		if err != nil {
			return nil, err
		}

		return types.NewDocumentValue(buf), nil
	default:
	}

	return nil, nil
}

// MarshalJSON implements the json.Marshaler interface.
func ValueToJSON(v types.Value) ([]byte, error) {
	switch v.Type() {
	case types.NullValue:
		return []byte("null"), nil
	case types.BoolValue:
		return strconv.AppendBool(nil, v.V().(bool)), nil
	case types.IntegerValue:
		return strconv.AppendInt(nil, v.V().(int64), 10), nil
	case types.DoubleValue:
		f := v.V().(float64)
		abs := math.Abs(f)
		fmt := byte('f')
		if abs != 0 {
			if abs < 1e-6 || abs >= 1e21 {
				fmt = 'e'
			}
		}

		// By default the precision is -1 to use the smallest number of digits.
		// See https://pkg.go.dev/strconv#FormatFloat
		prec := -1

		return strconv.AppendFloat(nil, v.V().(float64), fmt, prec, 64), nil
	case types.TextValue:
		return []byte(strconv.Quote(v.V().(string))), nil
	case types.BlobValue:
		src := v.V().([]byte)
		dst := make([]byte, base64.StdEncoding.EncodedLen(len(src))+2)
		dst[0] = '"'
		dst[len(dst)-1] = '"'
		base64.StdEncoding.Encode(dst[1:], src)
		return dst, nil
	case types.ArrayValue:
		return JsonArray{Array: v.V().(types.Array)}.MarshalJSON()
	case types.DocumentValue:
		return JsonDocument{Document: v.V().(types.Document)}.MarshalJSON()
	default:
		return nil, stringutil.Errorf("unexpected type: %d", v.Type())
	}
}

type JsonArray struct {
	types.Array
}

func (j JsonArray) MarshalJSON() ([]byte, error) {
	var buf bytes.Buffer

	buf.WriteByte('[')
	var notFirst bool
	err := j.Array.Iterate(func(i int, v types.Value) error {
		if notFirst {
			buf.WriteString(", ")
		}
		notFirst = true

		data, err := ValueToJSON(v)
		if err != nil {
			return err
		}

		_, err = buf.Write(data)
		return err
	})
	if err != nil {
		return nil, err
	}
	buf.WriteByte(']')
	return buf.Bytes(), nil
}

type JsonDocument struct {
	types.Document
}

func (j JsonDocument) MarshalJSON() ([]byte, error) {
	var buf bytes.Buffer

	buf.WriteByte('{')

	var notFirst bool
	err := j.Document.Iterate(func(f string, v types.Value) error {
		if notFirst {
			buf.WriteString(", ")
		}
		notFirst = true

		buf.WriteString(strconv.Quote(f))
		buf.WriteString(": ")

		data, err := ValueToJSON(v)
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

func ValueToString(v types.Value) string {
	switch v.Type() {
	case types.NullValue:
		return "NULL"
	case types.TextValue:
		return "'" + strings.ReplaceAll(v.V().(string), "'", "\\'") + "'"
	}

	d, _ := ValueToJSON(v)
	return string(d)
}
