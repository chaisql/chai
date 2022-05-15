package document

import (
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/genjidb/genji/types"
)

// A Path represents the path to a particular value within a document.
type Path []PathFragment

// NewPath creates a path from a list of strings representing either a field name
// or an array index in string form.
func NewPath(fragments ...string) Path {
	var path Path

	for _, frag := range fragments {
		idx, err := strconv.Atoi(frag)
		if err != nil {
			path = append(path, PathFragment{FieldName: frag})
		} else {
			path = append(path, PathFragment{ArrayIndex: idx})
		}
	}

	return path
}

// PathFragment is a fragment of a path representing either a field name or
// the index of an array.
type PathFragment struct {
	FieldName  string
	ArrayIndex int
}

// String representation of all the fragments of the path.
// It implements the Stringer interface.
func (p Path) String() string {
	var b strings.Builder

	for i := range p {
		if p[i].FieldName != "" {
			if i != 0 {
				b.WriteRune('.')
			}
			b.WriteString(p[i].FieldName)
		} else {
			b.WriteString("[" + strconv.Itoa(p[i].ArrayIndex) + "]")
		}
	}
	return b.String()
}

// IsEqual returns whether other is equal to p.
func (p Path) IsEqual(other Path) bool {
	if len(other) != len(p) {
		return false
	}

	for i := range p {
		if other[i] != p[i] {
			return false
		}
	}

	return true
}

// GetValueFromDocument returns the value at path p from d.
func (p Path) GetValueFromDocument(d types.Document) (types.Value, error) {
	if len(p) == 0 {
		return nil, errors.WithStack(types.ErrFieldNotFound)
	}
	if p[0].FieldName == "" {
		return nil, errors.WithStack(types.ErrFieldNotFound)
	}

	v, err := d.GetByField(p[0].FieldName)
	if err != nil {
		return nil, err
	}

	if len(p) == 1 {
		return v, nil
	}

	return p[1:].getValueFromValue(v)
}

// GetValueFromArray returns the value at path p from a.
func (p Path) GetValueFromArray(a types.Array) (types.Value, error) {
	if len(p) == 0 {
		return nil, errors.WithStack(types.ErrFieldNotFound)
	}
	if p[0].FieldName != "" {
		return nil, errors.WithStack(types.ErrFieldNotFound)
	}

	v, err := a.GetByIndex(p[0].ArrayIndex)
	if err != nil {
		if errors.Is(err, types.ErrValueNotFound) {
			return nil, errors.WithStack(types.ErrFieldNotFound)
		}

		return nil, err
	}

	if len(p) == 1 {
		return v, nil
	}

	return p[1:].getValueFromValue(v)
}

func (p Path) Clone() Path {
	c := make(Path, len(p))
	copy(c, p)
	return c
}

// Extend clones the path and appends the fragment to it.
func (p Path) Extend(f ...PathFragment) Path {
	c := make(Path, len(p)+len(f))
	copy(c, p)
	for i := range f {
		c[len(p)+i] = f[i]
	}
	return c
}

// Extend clones the path and appends the field to it.
func (p Path) ExtendField(field string) Path {
	return p.Extend(PathFragment{FieldName: field})
}

// Extend clones the path and appends the array index to it.
func (p Path) ExtendIndex(index int) Path {
	return p.Extend(PathFragment{ArrayIndex: index})
}

func (p Path) getValueFromValue(v types.Value) (types.Value, error) {
	switch v.Type() {
	case types.DocumentValue:
		return p.GetValueFromDocument(types.As[types.Document](v))
	case types.ArrayValue:
		return p.GetValueFromArray(types.As[types.Array](v))
	}

	return nil, types.ErrFieldNotFound
}

type Paths []Path

func (p Paths) String() string {
	var sb strings.Builder

	for i, pt := range p {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(pt.String())
	}

	return sb.String()
}

// IsEqual returns whether other is equal to p.
func (p Paths) IsEqual(other Paths) bool {
	if len(other) != len(p) {
		return false
	}

	for i := range p {
		if !other[i].IsEqual(p[i]) {
			return false
		}
	}

	return true
}
