package recordutil

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"

	"github.com/asdine/genji/record"
)

// DumpRecord is helper that dumps the name, type and value of each field of a record into the given writer.
func DumpRecord(w io.Writer, r record.Record) error {
	return r.Iterate(func(f record.Field) error {
		v, err := f.Decode()
		fmt.Fprintf(w, "%s(%s): %#v\n", f.Name, f.Type, v)
		return err
	})
}

// RecordToJSON encodes r to w in JSON.
func RecordToJSON(w io.Writer, r record.Record) error {
	return json.NewEncoder(w).Encode(jsonRecord{r})
}

type jsonRecord struct {
	record.Record
}

func (j jsonRecord) MarshalJSON() ([]byte, error) {
	var buf bytes.Buffer

	buf.WriteByte('{')

	var notFirst bool
	err := j.Record.Iterate(func(f record.Field) error {
		if notFirst {
			buf.WriteByte(',')
		}
		notFirst = true

		v, err := f.Decode()
		if err != nil {
			return err
		}

		buf.WriteByte('"')
		buf.WriteString(f.Name)
		buf.WriteString(`":`)

		mv, err := json.Marshal(v)
		if err != nil {
			return err
		}
		buf.Write(mv)
		return nil
	})
	if err != nil {
		return nil, err
	}

	buf.WriteByte('}')

	return buf.Bytes(), nil
}

// IteratorToCSV encodes all the records of an iterator to CSV.
func IteratorToCSV(w io.Writer, s record.Stream) error {
	cw := csv.NewWriter(w)

	var line []string
	err := s.Iterate(func(r record.Record) error {
		line = line[:0]

		err := r.Iterate(func(f record.Field) error {
			v, err := f.Decode()
			if err != nil {
				return err
			}

			line = append(line, fmt.Sprintf("%v", v))

			return err
		})
		if err != nil {
			return err
		}

		return cw.Write(line)
	})
	if err != nil {
		return err
	}

	cw.Flush()
	return nil
}

// IteratorToJSON encodes all the records of an iterator to JSON stream.
func IteratorToJSON(w io.Writer, s record.Stream) error {
	enc := json.NewEncoder(w)

	return s.Iterate(func(r record.Record) error {
		return enc.Encode(jsonRecord{r})
	})
}

// Scan a record into the given variables. Each variable must be a pointer to
// types supported by Genji.
func Scan(r record.Record, targets ...interface{}) error {
	var i int

	return r.Iterate(func(f record.Field) error {
		if i >= len(targets) {
			return errors.New("target list too small")
		}

		ref := reflect.ValueOf(targets[i])

		if !ref.IsValid() || ref.Kind() != reflect.Ptr {
			return errors.New("target must be pointer to a valid Go type")
		}

		switch t := targets[i].(type) {
		case *uint:
			x, err := f.DecodeToUint()
			if err != nil {
				return err
			}

			*t = x
		case *uint8:
			x, err := f.DecodeToUint8()
			if err != nil {
				return err
			}

			*t = x
		case *uint16:
			x, err := f.DecodeToUint16()
			if err != nil {
				return err
			}

			*t = x
		case *uint32:
			x, err := f.DecodeToUint32()
			if err != nil {
				return err
			}

			*t = x
		case *uint64:
			x, err := f.DecodeToUint64()
			if err != nil {
				return err
			}

			*t = x
		case *int:
			x, err := f.DecodeToInt()
			if err != nil {
				return err
			}

			*t = x
		case *int8:
			x, err := f.DecodeToInt8()
			if err != nil {
				return err
			}

			*t = x
		case *int16:
			x, err := f.DecodeToInt16()
			if err != nil {
				return err
			}

			*t = x
		case *int32:
			x, err := f.DecodeToInt32()
			if err != nil {
				return err
			}

			*t = x
		case *int64:
			x, err := f.DecodeToInt64()
			if err != nil {
				return err
			}

			*t = x
		case *float32:
			x, err := f.DecodeToFloat32()
			if err != nil {
				return err
			}

			*t = x
		case *float64:
			x, err := f.DecodeToFloat64()
			if err != nil {
				return err
			}

			*t = x
		case *string:
			x, err := f.DecodeToString()
			if err != nil {
				return err
			}

			*t = x
		case *[]byte:
			x, err := f.DecodeToBytes()
			if err != nil {
				return err
			}

			*t = x
		case *bool:
			x, err := f.DecodeToBool()
			if err != nil {
				return err
			}

			*t = x
		default:
			return errors.New("unsupported type")
		}
		i++
		return nil
	})
}
