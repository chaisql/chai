package stringutil

import (
	"errors"
	"io"
	"strconv"
	"strings"
)

type Stringer interface {
	String() string
}

func fprintf(w io.Writer, msg string, args ...interface{}) (int, error) {
	s := sprintf(msg, args...)
	return w.Write([]byte(s))
}

func sprintf(msg string, args ...interface{}) string {
	var count int
	var b strings.Builder

	for i := 0; i < len(msg); i++ {
		if msg[i] == '%' {
			if i+1 < len(msg) {
				switch msg[i+1] {
				case 's', 'd', 'v', 'w', 'c':
					b.WriteString(toString(args[count]))
					i++
					count++

					continue
				case 'q':
					b.WriteString(strconv.Quote(toString(args[count])))
					i++
					count++

					continue
				}
			}
		}

		b.WriteByte(msg[i])
	}

	return b.String()
}

func toString(v interface{}) string {
	switch t := v.(type) {
	case byte:
		return string(t)
	case rune:
		return string(t)
	case string:
		return t
	case []byte:
		var b strings.Builder
		b.WriteByte('[')
		for i := range t {
			if i > 0 {
				b.WriteString(", ")
			}
			b.WriteString(strconv.Itoa(int(t[i])))
		}
		b.WriteByte(']')
		return b.String()
	case Stringer:
		return t.String()
	case int:
		return strconv.Itoa(t)
	case int64:
		return strconv.Itoa(int(t))
	case uint64:
		return strconv.Itoa(int(t))
	case error:
		return t.Error()
	default:
		panic("incompatible type")
	}
}

func errorf(msg string, args ...interface{}) error {
	return errors.New(sprintf(msg, args...))
}
