// +build debug

package errors

import (
	"bytes"
	"fmt"
	"io"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/genjidb/genji/internal/stringutil"
	"github.com/stretchr/testify/require"
)

var currentFilename string

func init() {
	_, path, _, _ := runtime.Caller(0)
	currentFilename = filepath.Base(path)
}

func TestStackFormat(t *testing.T) {
	defer func() {
		err := recover()
		if err != 'a' {
			t.Fatal(err)
		}
		e, expected := errorf("hi"), callers()
		bs := [][]uintptr{e.stack, expected}
		if err := compareStacks(bs[0], bs[1]); err != nil {
			t.Errorf("Stack didn't match")
			t.Errorf(err.Error())
		}
		stack := string(e.Stack())
		if !strings.Contains(stack, "a: b(5)") {
			t.Errorf("Stack trace does not contain source line: 'a: b(5)'")
			t.Errorf(stack)
		}
		if !strings.Contains(stack, currentFilename+":") {
			t.Errorf("Stack trace does not contain file name: '%s:'", currentFilename)
			t.Errorf(stack)
		}
	}()
	_ = a()
}

func TestSkipWorks(t *testing.T) {
	defer func() {
		err := recover()
		if err != 'a' {
			t.Fatal(err)
		}
		bs := [][]uintptr{wrap("hi", 2).stack, callersSkip(2)}
		if err := compareStacks(bs[0], bs[1]); err != nil {
			t.Errorf("Stack didn't match")
			t.Errorf(err.Error())
		}
	}()
	_ = a()
}

func TestNew(t *testing.T) {
	err := _new("foo")
	if err.Error() != "foo" {
		t.Errorf("Wrong message")
	}
	err = _new(stringutil.Errorf("foo"))
	if err.Error() != "foo" {
		t.Errorf("Wrong message")
	}
	bs := [][]uintptr{_new("foo").stack, callers()}
	if err := compareStacks(bs[0], bs[1]); err != nil {
		t.Errorf("Stack didn't match")
		t.Errorf(err.Error())
	}
	if err.ErrorStack() != err.TypeName()+" "+err.Error()+"\n"+string(err.Stack()) {
		t.Errorf("ErrorStack is in the wrong format")
	}
}

func TestIs(t *testing.T) {
	if Is(nil, io.EOF) {
		t.Errorf("nil is an error")
	}
	if !Is(io.EOF, io.EOF) {
		t.Errorf("io.EOF is not io.EOF")
	}
	if !Is(_new(io.EOF), io.EOF) {
		t.Errorf("_new(io.EOF) is not io.EOF")
	}
	if !Is(io.EOF, _new(io.EOF)) {
		t.Errorf("io.EOF is not New(io.EOF)")
	}
	if !Is(_new(io.EOF), _new(io.EOF)) {
		t.Errorf("New(io.EOF) is not New(io.EOF)")
	}
	if Is(io.EOF, fmt.Errorf("io.EOF")) {
		t.Errorf("io.EOF is fmt.Errorf")
	}
}

func TestRequireIsError(t *testing.T) {
	require.ErrorIs(t, _new(io.EOF), io.EOF)
}

func TestWrapError(t *testing.T) {
	e := func() error {
		return wrap("hi", 1)
	}()
	if e.Error() != "hi" {
		t.Errorf("Constructor with a string failed")
	}
	if wrap(fmt.Errorf("yo"), 0).Error() != "yo" {
		t.Errorf("Constructor with an error failed")
	}
	if wrap(e, 0) != e {
		t.Errorf("Constructor with an Error failed")
	}
	if wrap(nil, 0) != nil {
		t.Errorf("Constructor with nil failed")
	}
}

func a() error {
	b(5)
	return nil
}

func b(i int) {
	c()
}

func c() {
	panic('a')
}

// compareStacks will compare a stack created using the errors package (actual)
// with a reference stack created with the callers function (expected). The
// first entry is not compared  since the actual and expected stacks cannot
// be created at the exact same program counter position so the first entry
// will always differ somewhat. Returns nil if the stacks are equal enough and
// an error containing a detailed error message otherwise.
func compareStacks(actual, expected []uintptr) error {
	if len(actual) != len(expected) {
		return stackCompareError("Stacks does not have equal length", actual, expected)
	}
	for i, pc := range actual {
		if i != 0 && pc != expected[i] {
			return stackCompareError(fmt.Sprintf("Stacks does not match entry %d (and maybe others)", i), actual, expected)
		}
	}
	return nil
}

func stackCompareError(msg string, actual, expected []uintptr) error {
	return fmt.Errorf("%s\nActual stack trace:\n%s\nExpected stack trace:\n%s", msg, readableStackTrace(actual), readableStackTrace(expected))
}

func callers() []uintptr {
	return callersSkip(1)
}

func callersSkip(skip int) []uintptr {
	callers := make([]uintptr, MaxStackDepth)
	length := runtime.Callers(skip+2, callers[:])
	return callers[:length]
}

func readableStackTrace(callers []uintptr) string {
	var result bytes.Buffer
	frames := callersToFrames(callers)
	for _, frame := range frames {
		result.WriteString(fmt.Sprintf("%s:%d (%#x)\n\t%s\n", frame.File, frame.Line, frame.PC, frame.Function))
	}
	return result.String()
}

func callersToFrames(callers []uintptr) []runtime.Frame {
	frames := make([]runtime.Frame, 0, len(callers))
	framesPtr := runtime.CallersFrames(callers)
	for {
		frame, more := framesPtr.Next()
		frames = append(frames, frame)
		if !more {
			return frames
		}
	}
}
