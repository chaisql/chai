package q

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

type Matcher struct {
	fn func(record.Record) (bool, error)
}

func (m *Matcher) Match(r record.Record) (bool, error) {
	return m.fn(r)
}

type IndexMatcher struct {
	*Matcher

	fn func(im map[string]index.Index) ([][]byte, error)
}

func (m *IndexMatcher) MatchIndex(im map[string]index.Index) ([][]byte, error) {
	return m.fn(im)
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

func EqInt(f Field, i int) *IndexMatcher {
	data := field.EncodeInt64(int64(i))

	return &IndexMatcher{
		Matcher: &Matcher{
			fn: compareInts(f, i, func(a, b int) bool {
				return a == b
			}),
		},

		fn: func(im map[string]index.Index) ([][]byte, error) {
			idx := im[f.Name()]
			c := idx.Cursor()
			v, rowid := c.Seek(data)
			var rowids [][]byte
			for rowid != nil && bytes.Equal(data, v) {
				rowids = append(rowids, rowid)
				rowid, v = c.Next()
			}

			return rowids, nil
		},
	}
}

func GtInt(f Field, i int) *IndexMatcher {
	data := field.EncodeInt64(int64(i))

	return &IndexMatcher{
		Matcher: &Matcher{
			fn: compareInts(f, i, func(a, b int) bool {
				return a > b
			}),
		},

		fn: func(im map[string]index.Index) ([][]byte, error) {
			idx := im[f.Name()]
			c := idx.Cursor()
			v, rowid := c.Seek(data)
			var rowids [][]byte
			for rowid != nil {
				if !bytes.Equal(data, v) {
					rowids = append(rowids, rowid)
				}

				rowid, _ = c.Next()
			}

			return rowids, nil
		},
	}
}

func GteInt(f Field, i int) *IndexMatcher {
	data := field.EncodeInt64(int64(i))

	return &IndexMatcher{
		Matcher: &Matcher{
			fn: compareInts(f, i, func(a, b int) bool {
				return a >= b
			}),
		},

		fn: func(im map[string]index.Index) ([][]byte, error) {
			idx := im[f.Name()]
			c := idx.Cursor()
			_, rowid := c.Seek(data)
			var rowids [][]byte
			for rowid != nil {
				rowids = append(rowids, rowid)
				rowid, _ = c.Next()
			}

			return rowids, nil
		},
	}
}

func LtInt(f Field, i int) *IndexMatcher {
	data := field.EncodeInt64(int64(i))

	return &IndexMatcher{
		Matcher: &Matcher{
			fn: compareInts(f, i, func(a, b int) bool {
				return a < b
			}),
		},

		fn: func(im map[string]index.Index) ([][]byte, error) {
			idx := im[f.Name()]
			c := idx.Cursor()
			v, rowid := c.Seek(data)
			rowid, v = c.Prev()
			var rowids [][]byte
			for rowid != nil {
				if !bytes.Equal(data, v) {
					rowids = append(rowids, rowid)
				}
				rowid, v = c.Prev()
			}

			return rowids, nil
		},
	}
}

func LteInt(f Field, i int) *IndexMatcher {
	data := field.EncodeInt64(int64(i))

	return &IndexMatcher{
		Matcher: &Matcher{
			fn: compareInts(f, i, func(a, b int) bool {
				return a <= b
			}),
		},

		fn: func(im map[string]index.Index) ([][]byte, error) {
			idx := im[f.Name()]
			c := idx.Cursor()
			v, rowid := c.Seek(data)
			if rowid == nil {
				rowid, v = c.Prev()
			}

			var pick bool
			if bytes.Equal(data, v) {
				pick = true
			}

			var rowids [][]byte
			for rowid != nil {
				if pick {
					rowids = append(rowids, rowid)
				} else if bytes.Compare(data, v) < 0 {
					pick = true
				}

				rowid, v = c.Prev()
			}

			return rowids, nil
		},
	}
}
