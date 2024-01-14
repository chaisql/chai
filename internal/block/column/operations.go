package column

import "unsafe"

//go:generate gocc c/operations_arm64.c --arch apple -O3 --package column

func Int64AddConstant(dest []int64, a []int64, x int64) {
	int64_add_scalar(unsafe.Pointer(&a[0]), x, unsafe.Pointer(&dest[0]), int32(len(a)))
}

func Int64SubConstant(dest []int64, a []int64, x int64) {
	int64_sub_scalar(unsafe.Pointer(&a[0]), x, unsafe.Pointer(&dest[0]), int32(len(a)))
}

func Int64MulConstant(dest []int64, a []int64, x int64) {
	int64_mul_scalar(unsafe.Pointer(&a[0]), x, unsafe.Pointer(&dest[0]), int32(len(a)))
}

func Int64DivConstant(dest []int64, a []int64, x int64) {
	int64_div_scalar(unsafe.Pointer(&a[0]), x, unsafe.Pointer(&dest[0]), int32(len(a)))
}

func Int64ModConstant(dest []int64, a []int64, x int64) {
	int64_mod_scalar(unsafe.Pointer(&a[0]), x, unsafe.Pointer(&dest[0]), int32(len(a)))
}

func Int32AddConstant(dest []int32, a []int32, x int32) {
	int32_add_scalar(unsafe.Pointer(&a[0]), x, unsafe.Pointer(&dest[0]), int32(len(a)))
}

func Int32SubConstant(dest []int32, a []int32, x int32) {
	int32_sub_scalar(unsafe.Pointer(&a[0]), x, unsafe.Pointer(&dest[0]), int32(len(a)))
}

func Int32MulConstant(dest []int32, a []int32, x int32) {
	int32_mul_scalar(unsafe.Pointer(&a[0]), x, unsafe.Pointer(&dest[0]), int32(len(a)))
}

func Int32DivConstant(dest []int32, a []int32, x int32) {
	int32_div_scalar(unsafe.Pointer(&a[0]), x, unsafe.Pointer(&dest[0]), int32(len(a)))
}

func Int32ModConstant(dest []int32, a []int32, x int32) {
	int32_mod_scalar(unsafe.Pointer(&a[0]), x, unsafe.Pointer(&dest[0]), int32(len(a)))
}
