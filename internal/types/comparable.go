package types

import (
	"strings"
)

type Comparable interface {
	EQ(other Value) (bool, error)
	GT(other Value) (bool, error)
	GTE(other Value) (bool, error)
	LT(other Value) (bool, error)
	LTE(other Value) (bool, error)
	Between(a, b Value) (bool, error)
}

type operator uint8

const (
	operatorEq operator = iota + 1
	operatorGt
	operatorGte
	operatorLt
	operatorLte
)

func compareArrays(op operator, l Array, r Array) (bool, error) {
	var i, j int

	for {
		lv, lerr := l.GetByIndex(i)
		rv, rerr := r.GetByIndex(j)
		if lerr == nil {
			i++
		}
		if rerr == nil {
			j++
		}
		if lerr != nil || rerr != nil {
			break
		}

		if lv.Type().IsComparableWith(rv.Type()) {
			isEq, err := lv.EQ(rv)
			if err != nil {
				return false, err
			}
			if !isEq {
				switch op {
				case operatorEq:
					return false, nil
				case operatorGt:
					return lv.GT(rv)
				case operatorGte:
					return lv.GTE(rv)
				case operatorLt:
					return lv.LT(rv)
				case operatorLte:
					return lv.LTE(rv)
				}
			}
		} else {
			switch op {
			case operatorEq:
				return false, nil
			case operatorGt, operatorGte:
				return lv.Type() > rv.Type(), nil
			case operatorLt, operatorLte:
				return lv.Type() < rv.Type(), nil
			}
		}
	}

	switch {
	case i > j:
		switch op {
		case operatorEq, operatorLt, operatorLte:
			return false, nil
		default:
			return true, nil
		}
	case i < j:
		switch op {
		case operatorEq, operatorGt, operatorGte:
			return false, nil
		default:
			return true, nil
		}
	default:
		switch op {
		case operatorEq, operatorGte, operatorLte:
			return true, nil
		default:
			return false, nil
		}
	}
}

func compareObjects(op operator, l, r Object) (bool, error) {
	lf, err := Fields(l)
	if err != nil {
		return false, err
	}
	rf, err := Fields(r)
	if err != nil {
		return false, err
	}

	if len(lf) == 0 && len(rf) > 0 {
		switch op {
		case operatorEq:
			return false, nil
		case operatorGt:
			return false, nil
		case operatorGte:
			return false, nil
		case operatorLt:
			return true, nil
		case operatorLte:
			return true, nil
		}
	}

	if len(rf) == 0 && len(lf) > 0 {
		switch op {
		case operatorEq:
			return false, nil
		case operatorGt:
			return true, nil
		case operatorGte:
			return true, nil
		case operatorLt:
			return false, nil
		case operatorLte:
			return false, nil
		}
	}

	var i, j int

	for i < len(lf) && j < len(rf) {
		if cmp := strings.Compare(lf[i], rf[j]); cmp != 0 {
			switch op {
			case operatorEq:
				return false, nil
			case operatorGt:
				return cmp > 0, nil
			case operatorGte:
				return cmp >= 0, nil
			case operatorLt:
				return cmp < 0, nil
			case operatorLte:
				return cmp <= 0, nil
			}
		}

		lv, lerr := l.GetByField(lf[i])
		rv, rerr := r.GetByField(rf[j])
		if lerr == nil {
			i++
		}
		if rerr == nil {
			j++
		}
		if lerr != nil || rerr != nil {
			break
		}

		if lv.Type().IsComparableWith(rv.Type()) {
			isEq, err := lv.EQ(rv)
			if err != nil {
				return false, err
			}
			if !isEq {
				switch op {
				case operatorEq:
					return false, nil
				case operatorGt:
					return lv.GT(rv)
				case operatorGte:
					return lv.GTE(rv)
				case operatorLt:
					return lv.LT(rv)
				case operatorLte:
					return lv.LTE(rv)
				}
			}
		} else {
			switch op {
			case operatorEq:
				return false, nil
			case operatorGt, operatorGte:
				return lv.Type() > rv.Type(), nil
			case operatorLt, operatorLte:
				return lv.Type() < rv.Type(), nil
			}
		}
	}

	switch {
	case i > j:
		switch op {
		case operatorEq, operatorLt, operatorLte:
			return false, nil
		default:
			return true, nil
		}
	case i < j:
		switch op {
		case operatorEq, operatorGt, operatorGte:
			return false, nil
		default:
			return true, nil
		}
	default:
		switch op {
		case operatorEq, operatorGte, operatorLte:
			return true, nil
		default:
			return false, nil
		}
	}
}
