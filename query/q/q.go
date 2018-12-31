package q

import (
	"errors"

	"github.com/asdine/genji/field"
	"github.com/asdine/genji/record"
)

type Field string

func (f Field) Name() string {
	return string(f)
}

type Matcher struct {
	fn func(record.Record) (bool, error)
}

func (m *Matcher) Match(r record.Record) (bool, error) {
	return m.fn(r)
}

func compareInts(f Field, i int, op func(a, b int) bool) func(r record.Record) (bool, error) {
	return func(r record.Record) (bool, error) {
		rf, err := r.Field(f.Name())
		if err != nil {
			return false, err
		}

		if rf.Type != field.Int64 {
			return false, errors.New("type mismatch")
		}

		v, err := field.DecodeInt64(rf.Data)
		if err != nil {
			return false, err
		}

		return op(int(v), i), nil
	}
}

func GtInt(f Field, i int) *Matcher {
	return &Matcher{fn: compareInts(f, i, func(a, b int) bool {
		return a > b
	})}
}

func GteInt(f Field, i int) *Matcher {
	return &Matcher{fn: compareInts(f, i, func(a, b int) bool {
		return a >= b
	})}
}

func LtInt(f Field, i int) *Matcher {
	return &Matcher{fn: compareInts(f, i, func(a, b int) bool {
		return a < b
	})}
}

func LteInt(f Field, i int) *Matcher {
	return &Matcher{fn: compareInts(f, i, func(a, b int) bool {
		return a <= b
	})}
}

func EqInt(f Field, i int) *Matcher {
	return &Matcher{fn: compareInts(f, i, func(a, b int) bool {
		return a == b
	})}
}
