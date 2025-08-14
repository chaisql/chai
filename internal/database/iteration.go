package database

import (
	"github.com/chaisql/chai/internal/tree"
	"github.com/chaisql/chai/internal/types"
)

type Pivot []types.Value

type Range struct {
	Min, Max  Pivot
	Exclusive bool
	Exact     bool
}

func (r *Range) ToTreeRange(constraints *ColumnConstraints, columns []string) (*tree.Range, error) {
	var rng tree.Range

	if len(r.Min) > 0 {
		rng.Min = tree.NewKey(r.Min...)
	}

	if len(r.Max) > 0 {
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
