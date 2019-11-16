package value

import (
	"bytes"
	"math"
)

type operator uint8

const (
	operatorEq operator = iota + 1
	operatorGt
	operatorGte
	operatorLt
	operatorLte
)

// IsEqual returns true if v is equal to the given value.
func (v Value) IsEqual(other Value) (bool, error) {
	return compare(operatorEq, v, other)
}

// IsGreaterThan returns true if v is greather than the given value.
func (v Value) IsGreaterThan(other Value) (bool, error) {
	return compare(operatorGt, v, other)
}

// IsGreaterThanOrEqual returns true if v is greather than or equal to the given value.
func (v Value) IsGreaterThanOrEqual(other Value) (bool, error) {
	return compare(operatorGte, v, other)
}

// IsLesserThan returns true if v is lesser than the given value.
func (v Value) IsLesserThan(other Value) (bool, error) {
	return compare(operatorLt, v, other)
}

// IsLesserThanOrEqual returns true if v is lesser than or equal to the given value.
func (v Value) IsLesserThanOrEqual(other Value) (bool, error) {
	return compare(operatorLte, v, other)
}

func compare(op operator, l, r Value) (bool, error) {
	// deal with nil
	if l.Type == Null || r.Type == Null {
		switch op {
		case operatorEq, operatorGte, operatorLte:
			return l.Type == r.Type, nil
		case operatorGt, operatorLt:
			return false, nil
		}
	}

	// if same type, or string and bytes, no conversion needed
	if l.Type == r.Type || (l.Type == String && r.Type == Bytes) || (r.Type == String && l.Type == Bytes) {
		var ok bool
		switch op {
		case operatorEq:
			ok = bytes.Equal(l.Data, r.Data)
		case operatorGt:
			ok = bytes.Compare(l.Data, r.Data) > 0
		case operatorGte:
			ok = bytes.Compare(l.Data, r.Data) >= 0
		case operatorLt:
			ok = bytes.Compare(l.Data, r.Data) < 0
		case operatorLte:
			ok = bytes.Compare(l.Data, r.Data) <= 0
		}

		return ok, nil
	}

	// uint64 numbers can be bigger than int64 and thus cannot be converted
	// to int64 without first checking if they can overflow.
	// if they do, the result of all the operations is already known
	if l.Type == Uint64 || r.Type == Uint64 {
		lv, err := l.Decode()
		if err != nil {
			return false, err
		}

		rv, err := r.Decode()
		if err != nil {
			return false, err
		}

		var ui uint64
		if l.Type == Uint64 {
			ui = lv.(uint64)
		} else if r.Type == Uint64 {
			ui = rv.(uint64)
		}
		if ui > math.MaxInt64 {
			switch op {
			case operatorEq:
				return false, nil
			case operatorGt:
				fallthrough
			case operatorGte:
				return l.Type == Uint64, nil
			case operatorLt:
				return r.Type == Uint64, nil
			case operatorLte:
				return r.Type == Uint64, nil
			}
		}
	}

	// integer OP integer
	if IsInteger(l.Type) && IsInteger(r.Type) {
		ai, err := l.DecodeToInt64()
		if err != nil {
			return false, err
		}

		bi, err := r.DecodeToInt64()
		if err != nil {
			return false, err
		}

		var ok bool

		switch op {
		case operatorEq:
			ok = ai == bi
		case operatorGt:
			ok = ai > bi
		case operatorGte:
			ok = ai >= bi
		case operatorLt:
			ok = ai < bi
		case operatorLte:
			ok = ai <= bi
		}

		return ok, nil
	}

	// number OP number
	if IsNumber(l.Type) && IsNumber(r.Type) {
		af, err := l.DecodeToFloat64()
		if err != nil {
			return false, err
		}

		bf, err := r.DecodeToFloat64()
		if err != nil {
			return false, err
		}

		var ok bool

		switch op {
		case operatorEq:
			ok = af == bf
		case operatorGt:
			ok = af > bf
		case operatorGte:
			ok = af >= bf
		case operatorLt:
			ok = af < bf
		case operatorLte:
			ok = af <= bf
		}

		return ok, nil
	}

	return false, nil
}
