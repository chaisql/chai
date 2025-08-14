package types

type Numeric interface {
	Value

	// Add u to v and return the result.
	// Only numeric values and booleans can be added together.
	Add(other Numeric) (Value, error)
	// Sub calculates v - u and returns the result.
	// Only numeric values and booleans can be calculated together.
	Sub(other Numeric) (Value, error)
	// Mul calculates v * u and returns the result.
	// Only numeric values and booleans can be calculated together.
	Mul(other Numeric) (Value, error)
	// Div calculates v / u and returns the result.
	// Only numeric values and booleans can be calculated together.
	// If both v and u are integers, the result will be an integer.
	Div(other Numeric) (Value, error)
	// Mod calculates v / u and returns the result.
	// Only numeric values and booleans can be calculated together.
	// If both v and u are integers, the result will be an integer.
	Mod(other Numeric) (Value, error)
}

type Integral interface {
	// BitwiseAnd calculates v & u and returns the result.
	// Only numeric values and booleans can be calculated together.
	// If both v and u are integers, the result will be an integer.
	BitwiseAnd(other Numeric) (Value, error)
	// BitwiseOr calculates v | u and returns the result.
	// Only numeric values and booleans can be calculated together.
	// If both v and u are integers, the result will be an integer.
	BitwiseOr(other Numeric) (Value, error)
	// BitwiseXor calculates v ^ u and returns the result.
	// Only numeric values and booleans can be calculated together.
	// If both v and u are integers, the result will be an integer.
	BitwiseXor(other Numeric) (Value, error)
}

func isMulOverflow[T int32 | int64](left, right, min, max T) bool {
	if right > 0 {
		if left > max/right {
			return true
		}
	} else {
		if left < min/right {
			return true
		}
	}

	return false
}

func isAddOverflow[T int32 | int64](left, right, min, max T) bool {
	if right > 0 {
		if left > max-right {
			return true
		}
	} else {
		if left < min-right {
			return true
		}
	}

	return false
}

func isSubOverflow[T int32 | int64](left, right, min, max T) bool {
	if right > 0 {
		if left < min+right {
			return true
		}
	} else {
		if left > max+right {
			return true
		}
	}

	return false
}
