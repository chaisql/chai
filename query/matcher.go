package query

import (
	"bytes"
	"errors"

	"github.com/asdine/genji/field"
	"github.com/asdine/genji/index"
	"github.com/asdine/genji/record"
)

type Field string

func (f Field) Name() string {
	return string(f)
}

type Matcher interface {
	Match(record.Record) (bool, error)
}

type matcher struct {
	fn func(record.Record) (bool, error)
}

func (m *matcher) Match(r record.Record) (bool, error) {
	return m.fn(r)
}

type IndexMatcher struct {
	Matcher

	fn func(im map[string]index.Index) ([][]byte, error)
}

func (m *IndexMatcher) MatchIndex(im map[string]index.Index) ([][]byte, error) {
	return m.fn(im)
}

func compareInts(f Field, op func(int64) bool) func(r record.Record) (bool, error) {
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

		return op(v), nil
	}
}

func compareStrings(f Field, op func([]byte) bool) func(r record.Record) (bool, error) {
	return func(r record.Record) (bool, error) {
		rf, err := r.Field(f.Name())
		if err != nil {
			return false, err
		}

		if rf.Type != field.String {
			return false, errors.New("type mismatch")
		}

		return op(rf.Data), nil
	}
}

func eqIndexMatcher(data []byte, idx index.Index) ([][]byte, error) {
	c := idx.Cursor()
	v, rowid := c.Seek(data)
	var rowids [][]byte
	for rowid != nil && bytes.Equal(data, v) {
		rowids = append(rowids, rowid)
		v, rowid = c.Next()
	}

	return rowids, nil
}

func gtIndexMatcher(data []byte, idx index.Index) ([][]byte, error) {
	c := idx.Cursor()
	v, rowid := c.Seek(data)
	var rowids [][]byte
	for rowid != nil {
		if !bytes.Equal(data, v) {
			rowids = append(rowids, rowid)
		}

		v, rowid = c.Next()
	}

	return rowids, nil
}

func gteIndexMatcher(data []byte, idx index.Index) ([][]byte, error) {
	c := idx.Cursor()
	_, rowid := c.Seek(data)
	var rowids [][]byte
	for rowid != nil {
		rowids = append(rowids, rowid)
		_, rowid = c.Next()
	}

	return rowids, nil
}

func ltIndexMatcher(data []byte, idx index.Index) ([][]byte, error) {
	c := idx.Cursor()
	v, rowid := c.Seek(data)
	v, rowid = c.Prev()
	var rowids [][]byte
	for rowid != nil {
		if !bytes.Equal(data, v) {
			rowids = append([][]byte{rowid}, rowids...)
		}
		v, rowid = c.Prev()
	}

	return rowids, nil
}

func lteIndexMatcher(data []byte, idx index.Index) ([][]byte, error) {
	c := idx.Cursor()
	v, rowid := c.Seek(data)

	for bytes.Equal(v, data) {
		v, rowid = c.Next()
	}
	if v == nil {
		v, rowid = c.Last()
	}

	var rowids [][]byte
	for rowid != nil {
		if bytes.Compare(v, data) <= 0 {
			rowids = append([][]byte{rowid}, rowids...)
		}

		v, rowid = c.Prev()
	}

	return rowids, nil
}

func EqInt(f Field, i int) *IndexMatcher {
	base := int64(i)
	return &IndexMatcher{
		Matcher: &matcher{
			fn: compareInts(f, func(v int64) bool {
				return v == base
			}),
		},

		fn: func(im map[string]index.Index) ([][]byte, error) {
			return eqIndexMatcher(field.EncodeInt64(base), im[f.Name()])
		},
	}
}

func GtInt(f Field, i int) *IndexMatcher {
	base := int64(i)
	return &IndexMatcher{
		Matcher: &matcher{
			fn: compareInts(f, func(v int64) bool {
				return v > base
			}),
		},

		fn: func(im map[string]index.Index) ([][]byte, error) {
			return gtIndexMatcher(field.EncodeInt64(base), im[f.Name()])
		},
	}
}

func GteInt(f Field, i int) *IndexMatcher {
	base := int64(i)
	return &IndexMatcher{
		Matcher: &matcher{
			fn: compareInts(f, func(v int64) bool {
				return v >= base
			}),
		},

		fn: func(im map[string]index.Index) ([][]byte, error) {
			return gteIndexMatcher(field.EncodeInt64(base), im[f.Name()])
		},
	}
}

func LtInt(f Field, i int) *IndexMatcher {
	base := int64(i)
	return &IndexMatcher{
		Matcher: &matcher{
			fn: compareInts(f, func(v int64) bool {
				return v < base
			}),
		},

		fn: func(im map[string]index.Index) ([][]byte, error) {
			return ltIndexMatcher(field.EncodeInt64(base), im[f.Name()])
		},
	}
}

func LteInt(f Field, i int) *IndexMatcher {
	base := int64(i)
	return &IndexMatcher{
		Matcher: &matcher{
			fn: compareInts(f, func(v int64) bool {
				return v <= base
			}),
		},

		fn: func(im map[string]index.Index) ([][]byte, error) {
			return lteIndexMatcher(field.EncodeInt64(base), im[f.Name()])
		},
	}
}

func EqStr(f Field, s string) *IndexMatcher {
	base := []byte(s)

	return &IndexMatcher{
		Matcher: &matcher{
			fn: compareStrings(f, func(v []byte) bool {
				return bytes.Equal(v, base)
			}),
		},

		fn: func(im map[string]index.Index) ([][]byte, error) {
			return eqIndexMatcher(base, im[f.Name()])
		},
	}
}

func GtStr(f Field, s string) *IndexMatcher {
	base := []byte(s)

	return &IndexMatcher{
		Matcher: &matcher{
			fn: compareStrings(f, func(v []byte) bool {
				return bytes.Compare(v, base) > 0
			}),
		},

		fn: func(im map[string]index.Index) ([][]byte, error) {
			return gtIndexMatcher(base, im[f.Name()])
		},
	}
}

func GteStr(f Field, s string) *IndexMatcher {
	base := []byte(s)

	return &IndexMatcher{
		Matcher: &matcher{
			fn: compareStrings(f, func(v []byte) bool {
				return bytes.Compare(v, base) >= 0
			}),
		},

		fn: func(im map[string]index.Index) ([][]byte, error) {
			return gteIndexMatcher(base, im[f.Name()])
		},
	}
}

func LtStr(f Field, s string) *IndexMatcher {
	base := []byte(s)

	return &IndexMatcher{
		Matcher: &matcher{
			fn: compareStrings(f, func(v []byte) bool {
				return bytes.Compare(v, base) < 0
			}),
		},

		fn: func(im map[string]index.Index) ([][]byte, error) {
			return ltIndexMatcher(base, im[f.Name()])
		},
	}
}

func LteStr(f Field, s string) *IndexMatcher {
	base := []byte(s)

	return &IndexMatcher{
		Matcher: &matcher{
			fn: compareStrings(f, func(v []byte) bool {
				return bytes.Compare(v, base) <= 0
			}),
		},

		fn: func(im map[string]index.Index) ([][]byte, error) {
			return lteIndexMatcher(base, im[f.Name()])
		},
	}
}
