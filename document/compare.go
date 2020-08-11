package document

import (
	"bytes"
	"strings"
	"time"
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
	case l.Type == NullValue || r.Type == NullValue:
		return compareWithNull(op, l, r)

	// compare booleans together
	case l.Type == BoolValue && r.Type == BoolValue:
		return compareBooleans(op, l.V.(bool), r.V.(bool)), nil

	// compare texts together
	case l.Type == TextValue && r.Type == TextValue:
		return compareTexts(op, l.V.(string), r.V.(string)), nil

	// compare blobs together
	case r.Type == BlobValue && l.Type == BlobValue:
		return compareBlobs(op, l.V.([]byte), r.V.([]byte)), nil

	// compare numbers together
	case l.Type.IsNumber() && r.Type.IsNumber():
		return compareNumbers(op, l, r)

	// compare durations together
	case l.Type == DurationValue && r.Type == DurationValue:
		return compareIntegers(op, int64(l.V.(time.Duration)), int64(r.V.(time.Duration))), nil
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

	return false, nil
}

func compareBooleans(op operator, a, b bool) bool {
	switch op {
	case operatorEq:
		return a == b
	case operatorGt:
		return a == true && b == false
	case operatorGte:
		return a == b || a == true
	case operatorLt:
		return a == false && b == true
	case operatorLte:
		return a == b || a == false
	}

	return false
}

func compareTexts(op operator, l, r string) bool {
	switch op {
	case operatorEq:
		return l == r
	case operatorGt:
		return strings.Compare(l, r) > 0
	case operatorGte:
		return strings.Compare(l, r) >= 0
	case operatorLt:
		return strings.Compare(l, r) < 0
	case operatorLte:
		return strings.Compare(l, r) <= 0
	}

	return false
}

func compareBlobs(op operator, l, r []byte) bool {
	switch op {
	case operatorEq:
		return bytes.Equal(l, r)
	case operatorGt:
		return bytes.Compare(l, r) > 0
	case operatorGte:
		return bytes.Compare(l, r) >= 0
	case operatorLt:
		return bytes.Compare(l, r) < 0
	case operatorLte:
		return bytes.Compare(l, r) <= 0
	}

	return false
}

func compareIntegers(op operator, l, r int64) bool {
	switch op {
	case operatorEq:
		return l == r
	case operatorGt:
		return l > r
	case operatorGte:
		return l >= r
	case operatorLt:
		return l < r
	case operatorLte:
		return l <= r
	}

	return false
}

func compareNumbers(op operator, l, r Value) (bool, error) {
	var err error

	l, err = l.CastAsDouble()
	if err != nil {
		return false, err
	}
	r, err = r.CastAsDouble()
	if err != nil {
		return false, err
	}

	af := l.V.(float64)
	bf := r.V.(float64)

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
