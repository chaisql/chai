package testutil

// KV is used to represent key-value pairs stored in tables or indexes.
type KV struct {
	Key, Value []byte
}

func Int64Ptr(n int64) *int64 { return &n }
