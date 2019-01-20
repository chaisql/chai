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

func And(matchers ...Matcher) *IndexMatcher {
	return &IndexMatcher{
		Matcher: &matcher{
			fn: func(r record.Record) (bool, error) {
				for _, m := range matchers {
					ok, err := m.Match(r)
					if !ok || err != nil {
						return ok, err
					}
				}

				return true, nil
			},
		},

		fn: func(im map[string]index.Index) ([][]byte, error) {
			var set [][]byte

			for _, m := range matchers {
				if i, ok := m.(*IndexMatcher); ok {
					rowids, err := i.MatchIndex(im)
					if err != nil {
						return nil, err
					}

					if len(rowids) == 0 {
						return nil, nil
					}

					if set == nil {
						set = rowids
						continue
					}

					set = intersection(set, rowids)
					if len(set) == 0 {
						return nil, nil
					}
				} else {
					return nil, nil
				}
			}

			return set, nil
		},
	}
}

func Or(matchers ...Matcher) *IndexMatcher {
	return &IndexMatcher{
		Matcher: &matcher{
			fn: func(r record.Record) (bool, error) {
				for _, m := range matchers {
					ok, err := m.Match(r)
					if err != nil {
						return false, err
					}

					if ok {
						return true, nil
					}
				}

				return false, nil
			},
		},

		fn: func(im map[string]index.Index) ([][]byte, error) {
			var set [][]byte

			for _, m := range matchers {
				if i, ok := m.(*IndexMatcher); ok {
					rowids, err := i.MatchIndex(im)
					if err != nil {
						return nil, err
					}

					if set == nil {
						set = rowids
						continue
					}

					set = union(set, rowids)
				} else {
					return nil, nil
				}
			}

			return set, nil
		},
	}
}

func intersection(s1, s2 [][]byte) [][]byte {
	var lower, bigger [][]byte
	if len(s1) < len(s2) {
		lower, bigger = s1, s2
	} else {
		lower, bigger = s2, s1
	}

	set := make([][]byte, 0, len(lower))

	for _, v := range lower {
		if binarySearch(bigger, v) {
			set = append(set, v)
		}
	}

	return set
}

func binarySearch(set [][]byte, v []byte) bool {
	if len(set) == 0 {
		return false
	}

	idx := len(set) / 2
	comp := bytes.Compare(set[idx], v)
	if comp < 0 {
		return binarySearch(set[idx+1:], v)
	} else if comp > 0 {
		return binarySearch(set[0:idx], v)
	}

	return true
}

func union(s1, s2 [][]byte) [][]byte {
	var lower, bigger [][]byte
	if len(s1) < len(s2) {
		lower, bigger = s1, s2
	} else {
		lower, bigger = s2, s1
	}

	set := make([][]byte, 0, len(s1)+len(s2))

	for _, v := range lower {
		for i := 0; i < len(bigger); i++ {
			switch bytes.Compare(bigger[i], v) {
			case -1:
				set = append(set, bigger[i])
			case 0:
				bigger = bigger[i+1:]
				break
			case 1:
				bigger = bigger[i:]
				break
			}
		}

		set = append(set, v)
	}

	set = append(set, bigger...)

	return set
}
