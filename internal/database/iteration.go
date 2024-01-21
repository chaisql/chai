package database

import (
	"math"

	"github.com/chaisql/chai/internal/object"
	"github.com/chaisql/chai/internal/tree"
	"github.com/chaisql/chai/internal/types"
)

type Pivot []types.Value

type Range struct {
	Min, Max  Pivot
	Exclusive bool
	Exact     bool
}

func (r *Range) ToTreeRange(constraints *FieldConstraints, paths []object.Path) (*tree.Range, error) {
	var rng tree.Range
	var err error

	if len(r.Min) > 0 {
		for i := range r.Min {
			r.Min[i], err = r.Convert(constraints, r.Min[i], paths[i], true)
			if err != nil {
				return nil, err
			}
		}

		rng.Min = tree.NewKey(r.Min...)
	}

	if len(r.Max) > 0 {
		for i := range r.Max {
			r.Max[i], err = r.Convert(constraints, r.Max[i], paths[i], false)
			if err != nil {
				return nil, err
			}
		}

		rng.Max = tree.NewKey(r.Max...)
	}

	if r.Exclusive && r.Exact {
		panic("exclusive and exact cannot both be true")
	}

	if r.Exact {
		if rng.Max != nil {
			panic("cannot use exact with a max range")
		}

		rng.Max = rng.Min
	}

	rng.Exclusive = r.Exclusive

	return &rng, nil
}

func (r *Range) Convert(constraints *FieldConstraints, v types.Value, p object.Path, isMin bool) (types.Value, error) {
	// ensure the operand satisfies all the constraints, index can work only on exact types.
	// if a number is encountered, try to convert it to the right type if and only if the conversion
	// is lossless.
	// if a timestamp is encountered, ensure the field constraint is also a timestamp, otherwise convert it to text.
	v, err := constraints.ConvertValueAtPath(p, v, func(v types.Value, path object.Path, targetType types.Type) (types.Value, error) {
		if v.Type() == types.TypeInteger && targetType == types.TypeDouble {
			return object.CastAsDouble(v)
		}

		if v.Type() == types.TypeDouble && targetType == types.TypeInteger {
			f := types.AsFloat64(v)
			if float64(int64(f)) == f {
				return object.CastAsInteger(v)
			}

			if r.Exact {
				return v, nil
			}

			// we want to convert a non rounded double to int in a way that preserves
			// comparison logic with the index. ex:
			// a > 1.1  -> a >= 2; exclusive -> false
			// a >= 1.1 -> a >= 2; exclusive -> false
			// a < 1.1  -> a < 2;  exclusive -> true
			// a <= 1.1 -> a < 2;  exclusive -> true
			// a BETWEEN 1.1 AND 2.2 -> a >= 2 AND a <= 3; exclusive -> false

			// First, we need to ceil the number. Ex: 1.1 -> 2
			v = types.NewIntegerValue(int64(math.Ceil(f)))

			// Next, we need to convert the boundaries
			if isMin {
				// (a > 1.1) or (a >= 1.1) must be transformed to (a >= 2)
				r.Exclusive = false
			} else {
				// (a < 1.1) or (a <= 1.1) must be transformed to (a < 2)
				// But there is an exception: if we are dealing with both min
				// and max boundaries, we are operating a BETWEEN operation,
				// meaning that we need to convert a BETWEEN 1.1 AND 2.2 to a >= 2 AND a <= 3,
				// and thus have to set exclusive to false.
				r.Exclusive = r.Min == nil || len(r.Min) == 0
			}
		}

		if v.Type() == types.TypeTimestamp && targetType == types.TypeText {
			return object.CastAsText(v)
		}

		return v, nil
	})

	return v, err
}

func (r *Range) IsEqual(other *Range) bool {
	if r.Exact != other.Exact {
		return false
	}

	if r.Exclusive != other.Exclusive {
		return false
	}

	if len(r.Min) != len(other.Min) {
		return false
	}

	if len(r.Max) != len(other.Max) {
		return false
	}

	for i := range r.Min {
		eq, err := r.Min[i].EQ(other.Min[i])
		if err != nil || !eq {
			return false
		}
	}

	for i := range r.Max {
		eq, err := r.Max[i].EQ(other.Max[i])
		if err != nil || !eq {
			return false
		}
	}

	return true
}
