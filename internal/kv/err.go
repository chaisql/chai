package kv

import "github.com/cockroachdb/errors"

// Common errors returned by the engine implementations.
var (
	// ErrKeyNotFound is returned when the targeted key doesn't exist.
	ErrKeyNotFound = errors.New("key not found")

	// ErrKeyAlreadyExists is returned when the targeted key already exists.
	ErrKeyAlreadyExists = errors.New("key already exists")
)
