package document

import (
	"bytes"
	"errors"
	"fmt"
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

func (op operator) String() string {
	switch op {
	case operatorEq:
		return "="
	case operatorGt:
		return ">"
	case operatorGte:
		return ">="
	case operatorLt:
		return "<"
	case operatorLte:
		return "<="
	}

	return ""
}

// IsEqual returns true if v is equal to the given value.
func (v Value) IsEqual(other Value) (bool, error) {
	return compare(operatorEq, v, other)
}

// IsNotEqual returns true if v is not equal to the given value.
func (v Value) IsNotEqual(other Value) (bool, error) {
	ok, err := v.IsEqual(other)
	if err != nil {
		return ok, err
	}

	return !ok, nil
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
	switch {
	// deal with nil
	case l.Type == NullValue:
		fallthrough
	case r.Type == NullValue:
		return compareWithNull(op, l, r)

	// compare documents together
	case l.Type == DocumentValue && r.Type == DocumentValue:
		return compareDocuments(op, l, r)

	// compare arrays together
	case l.Type == ArrayValue && r.Type == ArrayValue:
		return compareArrays(op, l, r)

	// if same type, or string and bytes, no conversion needed
	case l.Type == r.Type:
		fallthrough
	case l.Type == StringValue && r.Type == BytesValue:
		fallthrough
	case r.Type == StringValue && l.Type == BytesValue:
		return compareBytes(op, l, r)

	// integer OP integer
	case l.Type.IsInteger() && r.Type.IsInteger():
		return compareIntegers(op, l, r)

	// number OP number
	case l.Type.IsNumber() && r.Type.IsNumber():
		return compareNumbers(op, l, r)
	}

	return false, nil
}

func compareWithNull(op operator, l, r Value) (bool, error) {
	switch op {
	case operatorEq, operatorGte, operatorLte:
		return l.Type == r.Type, nil
	case operatorGt, operatorLt:
		return false, nil
	}

	return false, fmt.Errorf("unknown operator %v", op)
}

func compareBytes(op operator, l, r Value) (bool, error) {
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

func compareIntegers(op operator, l, r Value) (bool, error) {
	// uint64 numbers can be bigger than int64 and thus cannot be converted
	// to int64 without first checking if they can overflow.
	// if they do, the result of all the operations is already known
	if l.Type == Uint64Value || r.Type == Uint64Value {
		lv, err := l.Decode()
		if err != nil {
			return false, err
		}

		rv, err := r.Decode()
		if err != nil {
			return false, err
		}

		var ui uint64
		if l.Type == Uint64Value {
			ui = lv.(uint64)
		} else if r.Type == Uint64Value {
			ui = rv.(uint64)
		}
		if ui > math.MaxInt64 {
			switch op {
			case operatorEq:
				return false, nil
			case operatorGt:
				fallthrough
			case operatorGte:
				return l.Type == Uint64Value, nil
			case operatorLt:
				return r.Type == Uint64Value, nil
			case operatorLte:
				return r.Type == Uint64Value, nil
			}
		}
	}

	// integer OP integer
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

func compareNumbers(op operator, l, r Value) (bool, error) {
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

var errStop = errors.New("stop")

func compareDocuments(op operator, l, r Value) (bool, error) {
	if op != operatorEq {
		return false, fmt.Errorf("%q operator not supported for document comparison", op)
	}

	ld, err := l.DecodeToDocument()
	if err != nil {
		return false, err
	}

	rd, err := r.DecodeToDocument()
	if err != nil {
		return false, err
	}

	var lsize, rsize int
	err = ld.Iterate(func(field string, lv Value) error {
		lsize++
		return nil
	})
	if err != nil {
		return false, err
	}
	err = rd.Iterate(func(field string, lv Value) error {
		rsize++
		return nil
	})
	if err != nil {
		return false, err
	}

	if lsize != rsize {
		return false, nil
	}

	// if both empty documents
	if lsize == 0 {
		return true, nil
	}

	var ok bool

	err = ld.Iterate(func(field string, lv Value) error {
		rv, err := rd.GetByField(field)
		if err != nil {
			return err
		}

		ok, err = compare(op, lv, rv)
		if err != nil {
			return err
		}

		if !ok {
			return errStop
		}

		return nil
	})

	if err != nil && err != errStop {
		return false, err
	}

	return ok, nil
}

func compareArrays(op operator, l, r Value) (bool, error) {
	la, err := l.DecodeToArray()
	if err != nil {
		return false, err
	}

	ra, err := r.DecodeToArray()
	if err != nil {
		return false, err
	}

	var ok bool
	var i, j int

	for {
		lv, lerr := la.GetByIndex(i)
		rv, rerr := ra.GetByIndex(j)

		if lerr == nil {
			i++
		}
		if rerr == nil {
			j++
		}

		if lerr != nil || rerr != nil {
			break
		}

		isEq, err := compare(operatorEq, lv, rv)
		if err != nil {
			return false, err
		}

		if !isEq && op != operatorEq {
			fmt.Println(lv, op, rv, ", isEq =", isEq, ", err =", err)

			return compare(op, lv, rv)
		}

		if !isEq {
			return false, nil
		}

		ok = isEq
	}

	if op == operatorEq {
		return i == j, nil
	}

	// return last value stored in ok
	return ok, nil
}
