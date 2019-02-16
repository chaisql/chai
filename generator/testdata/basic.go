package testdata

// Basic mirrors the basic structure used in the generator tests.
// It is the simplest struct that can be used with Genji: No tags, no methods, no comments.
// This must not be generated.
type Basic struct {
	A    string
	B    int64
	C, D string
}
