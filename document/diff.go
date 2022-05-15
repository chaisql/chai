package document

import (
	"github.com/cockroachdb/errors"
	"github.com/genjidb/genji/types"
)

// Diff returns the operations needed to transform the first document into the second.
func Diff(d1, d2 types.Document) ([]Op, error) {
	return diff(nil, d1, d2)
}

func diff(path Path, d1, d2 types.Document) ([]Op, error) {
	var ops []Op
	f1, err := types.Fields(d1)
	if err != nil {
		return nil, err
	}

	f2, err := types.Fields(d2)
	if err != nil {
		return nil, err
	}

	var i, j int
	for {
		for i < len(f1) && (j >= len(f2) || f1[i] < f2[j]) {
			v, err := d1.GetByField(f1[j])
			if err != nil {
				return nil, err
			}
			ops = append(ops, NewDeleteOp(path.ExtendField(f1[i]), v))
			i++
		}

		for j < len(f2) && (i >= len(f1) || f1[i] > f2[j]) {
			v, err := d2.GetByField(f2[j])
			if err != nil {
				return nil, err
			}
			ops = append(ops, NewSetOp(path.ExtendField(f2[j]), v))
			j++
		}

		if i == len(f1) && j == len(f2) {
			break
		}

		v1, err := d1.GetByField(f1[i])
		if err != nil {
			return nil, err
		}

		v2, err := d2.GetByField(f2[j])
		if err != nil {
			return nil, err
		}

		if v1.Type() != v2.Type() {
			v, err := d2.GetByField(f2[j])
			if err != nil {
				return nil, err
			}
			ops = append(ops, NewSetOp(path.ExtendField(f2[j]), v))
		} else {
			switch v1.Type() {
			case types.DocumentValue:
				subOps, err := diff(append(path, PathFragment{FieldName: f1[i]}), types.As[types.Document](v1), types.As[types.Document](v2))
				if err != nil {
					return nil, err
				}
				ops = append(ops, subOps...)
			case types.ArrayValue:
				subOps, err := arrayDiff(append(path, PathFragment{FieldName: f1[i]}), types.As[types.Array](v1), types.As[types.Array](v2))
				if err != nil {
					return nil, err
				}
				ops = append(ops, subOps...)
			default:
				ok, err := types.IsEqual(v1, v2)
				if err != nil {
					return nil, err
				}
				if !ok {
					ops = append(ops, NewSetOp(path.ExtendField(f2[j]), v2))
				}
			}
		}
		i++
		j++
	}

	return ops, nil
}

func arrayDiff(path Path, a1, a2 types.Array) ([]Op, error) {
	var ops []Op

	var i int
	for {
		v1, err := a1.GetByIndex(i)
		nov1 := errors.Is(err, types.ErrFieldNotFound)
		if !nov1 && err != nil {
			return nil, err
		}

		v2, err := a2.GetByIndex(i)
		nov2 := errors.Is(err, types.ErrFieldNotFound)
		if !nov2 && err != nil {
			return nil, err
		}

		if nov1 && nov2 {
			break
		}

		if nov1 && !nov2 {
			ops = append(ops, NewSetOp(path.ExtendIndex(i), v2))
			i++
			continue
		}
		if !nov1 && nov2 {
			ops = append(ops, NewDeleteOp(path.ExtendIndex(i), v1))
			i++
			continue
		}

		if v1.Type() != v2.Type() {
			ops = append(ops, NewSetOp(path.ExtendIndex(i), v2))
			i++
			continue
		}

		switch v1.Type() {
		case types.DocumentValue:
			subOps, err := diff(append(path, PathFragment{ArrayIndex: i}), types.As[types.Document](v1), types.As[types.Document](v2))
			if err != nil {
				return nil, err
			}
			ops = append(ops, subOps...)
		case types.ArrayValue:
			subOps, err := arrayDiff(append(path, PathFragment{ArrayIndex: i}), types.As[types.Array](v1), types.As[types.Array](v2))
			if err != nil {
				return nil, err
			}
			ops = append(ops, subOps...)
		default:
			ok, err := types.IsEqual(v1, v2)
			if err != nil {
				return nil, err
			}
			if !ok {
				ops = append(ops, NewSetOp(path.ExtendIndex(i), v2))
			}
		}
		i++
	}

	return ops, nil
}

// Op represents a single operation on a document.
// It is returned by the Diff function.
type Op struct {
	Type  string
	Path  Path
	Value types.Value
}

func NewSetOp(path Path, v types.Value) Op {
	return newOp("set", path, v)
}

func NewDeleteOp(path Path, v types.Value) Op {
	return newOp("delete", path, v)
}

func newOp(op string, path Path, v types.Value) Op {
	return Op{
		Type:  op,
		Path:  path,
		Value: v,
	}
}

func (o *Op) MarshalBinary() ([]byte, error) {
	panic("not implemented") // TODO: Implement
}
