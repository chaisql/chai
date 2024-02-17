package row

import (
	"github.com/chaisql/chai/internal/types"
)

// Diff returns the operations needed to transform the first row into the second.
func Diff(r1, r2 Row) ([]Op, error) {
	var ops []Op
	f1, err := Columns(r1)
	if err != nil {
		return nil, err
	}

	f2, err := Columns(r2)
	if err != nil {
		return nil, err
	}

	var i, j int
	for {
		for i < len(f1) && (j >= len(f2) || f1[i] < f2[j]) {
			v, err := r1.Get(f1[j])
			if err != nil {
				return nil, err
			}
			ops = append(ops, NewDeleteOp(f1[i], v))
			i++
		}

		for j < len(f2) && (i >= len(f1) || f1[i] > f2[j]) {
			v, err := r2.Get(f2[j])
			if err != nil {
				return nil, err
			}
			ops = append(ops, NewSetOp(f2[j], v))
			j++
		}

		if i == len(f1) && j == len(f2) {
			break
		}

		v1, err := r1.Get(f1[i])
		if err != nil {
			return nil, err
		}

		v2, err := r2.Get(f2[j])
		if err != nil {
			return nil, err
		}

		if v1.Type() != v2.Type() {
			v, err := r2.Get(f2[j])
			if err != nil {
				return nil, err
			}
			ops = append(ops, NewSetOp(f2[j], v))
		} else {
			ok, err := v1.EQ(v2)
			if err != nil {
				return nil, err
			}
			if !ok {
				ops = append(ops, NewSetOp(f2[j], v2))
			}
		}
		i++
		j++
	}

	return ops, nil
}

// Op represents a single operation on an row.
// It is returned by the Diff function.
type Op struct {
	Type   string
	Column string
	Value  types.Value
}

func NewSetOp(column string, v types.Value) Op {
	return newOp("set", column, v)
}

func NewDeleteOp(column string, v types.Value) Op {
	return newOp("delete", column, v)
}

func newOp(op string, column string, v types.Value) Op {
	return Op{
		Type:   op,
		Column: column,
		Value:  v,
	}
}
