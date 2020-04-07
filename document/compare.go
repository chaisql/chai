package document

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/asdine/genji/pkg/bytesutil"
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

	// compare boolean and another value
	case l.Type == BoolValue || r.Type == BoolValue:
		return compareWithBool(op, l, r)

	// compare strings and bytes together
	case l.Type == TextValue && r.Type == TextValue:
		fallthrough
	case l.Type == BlobValue && r.Type == BlobValue:
		fallthrough
	case l.Type == TextValue && r.Type == BlobValue:
		fallthrough
	case r.Type == TextValue && l.Type == BlobValue:
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

// when comparing booleans with numbers, true equals 1 and false 0
// when comparing booleans with other types, the boolean is always smaller.
func compareWithBool(op operator, l, r Value) (bool, error) {
	// if comparing a boolean with something other than a number or another bool, always return false.
	if (!l.Type.IsNumber() && l.Type != BoolValue) || (!r.Type.IsNumber() && r.Type != BoolValue) {
		return false, nil
	}

	var a, b bool

	if l.Type == BoolValue {
		a = l.V.(bool)
	} else {
		a = l.IsTruthy()
	}

	if r.Type == BoolValue {
		b = r.V.(bool)
	} else {
		b = r.IsTruthy()
	}

	switch op {
	case operatorEq:
		return a == b, nil
	case operatorGt:
		return a == true && b == false, nil
	case operatorGte:
		return a == b || a == true, nil
	case operatorLt:
		return a == false && b == true, nil
	case operatorLte:
		return a == b || a == false, nil
	}

	return false, fmt.Errorf("unknown operator %v", op)
}

func compareBytes(op operator, l, r Value) (bool, error) {
	var ok bool

	switch op {
	case operatorEq:
		ok = bytes.Equal(l.V.([]byte), r.V.([]byte))
	case operatorGt:
		ok = bytesutil.CompareBytes(l.V.([]byte), r.V.([]byte)) > 0
	case operatorGte:
		ok = bytesutil.CompareBytes(l.V.([]byte), r.V.([]byte)) >= 0
	case operatorLt:
		ok = bytesutil.CompareBytes(l.V.([]byte), r.V.([]byte)) < 0
	case operatorLte:
		ok = bytesutil.CompareBytes(l.V.([]byte), r.V.([]byte)) <= 0
	}

	return ok, nil
}

func compareIntegers(op operator, l, r Value) (bool, error) {
	// integer OP integer
	ai, err := l.ConvertToInt64()
	if err != nil {
		return false, err
	}

	bi, err := r.ConvertToInt64()
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
	af, err := l.ConvertToFloat64()
	if err != nil {
		return false, err
	}

	bf, err := r.ConvertToFloat64()
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

	ld, err := l.ConvertToDocument()
	if err != nil {
		return false, err
	}

	rd, err := r.ConvertToDocument()
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
	la, err := l.ConvertToArray()
	if err != nil {
		return false, err
	}

	ra, err := r.ConvertToArray()
	if err != nil {
		return false, err
	}

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
			return compare(op, lv, rv)
		}

		if !isEq {
			return false, nil
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
