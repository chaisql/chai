//go:build !wasm
// +build !wasm

package errors

import (
	"bytes"
	"reflect"
)

// Error wraps any error with a stacktrace, speeding up the development process.
// Such errors are only returned when the error package is compiled with the "debug" build tag.
type Error struct {
	Err    error
	stack  []uintptr
	frames []StackFrame
}

// Error returns the underlying error's message.
func (err *Error) Error() string {
	return err.Err.Error()
}

// Return the underlying error.
func (err *Error) Unwrap() error {
	return err.Err
}

// Is returns true if err equals to the target or the error wrapped by the target.
func (err *Error) Is(target error) bool {
	if err == target {
		return true
	}
	if e, ok := target.(*Error); ok {
		return err.Err == e.Err
	}
	return false
}

// Stack returns the callstack formatted the same way that go does
// in runtime/debug.Stack()
func (err *Error) Stack() []byte {
	buf := bytes.Buffer{}
	for _, frame := range err.StackFrames() {
		buf.WriteString(frame.String())
	}
	return buf.Bytes()
}

// StackFrames returns an array of frames containing information about the
// stack.
func (err *Error) StackFrames() []StackFrame {
	if err.frames == nil {
		err.frames = make([]StackFrame, len(err.stack))

		for i, pc := range err.stack {
			err.frames[i] = NewStackFrame(pc)
		}
	}
	return err.frames
}

// ErrorStack returns a string that contains both the
// error message and the callstack.
func (err *Error) ErrorStack() string {
	return err.TypeName() + " " + err.Error() + "\n" + string(err.Stack())
}

// TypeName returns the type this error. e.g. *errors.stringError.
func (err *Error) TypeName() string {
	return reflect.TypeOf(err.Err).String()
}
