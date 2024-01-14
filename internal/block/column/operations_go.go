package column

import "golang.org/x/exp/constraints"

func AddConstant[T constraints.Integer](dest []T, a []T, x T) {
	for i := range a {
		dest[i] = a[i] + x
	}
}

func SubConstant[T constraints.Integer](dest []T, a []T, x T) {
	for i := range a {
		dest[i] = a[i] - x
	}
}

func MulConstant[T constraints.Integer](dest []T, a []T, x T) {
	for i := range a {
		dest[i] = a[i] * x
	}
}

func DivConstant[T constraints.Integer](dest []T, a []T, x T) {
	for i := range a {
		dest[i] = a[i] / x
	}
}

func ModConstant[T constraints.Integer](dest []T, a []T, x T) {
	for i := range a {
		dest[i] = a[i] % x
	}
}
