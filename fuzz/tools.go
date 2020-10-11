// +build tools

package fuzz

// Makes `go mod tidy` keep tool dependencies in go.mod and go.sum.

import (
	_ "github.com/dvyukov/go-fuzz/go-fuzz"
	_ "github.com/dvyukov/go-fuzz/go-fuzz-build"
)
