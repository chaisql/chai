package testdata

// unexportedBasic is like Basic except that it is unexported.
type unexportedBasic struct {
	A    string
	B    int64
	C, D int64
}
