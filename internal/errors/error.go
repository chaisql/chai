package errors

import (
	"bytes"
	baseErrors "errors"
	"reflect"
)

type Error struct {
	Err    error
	stack  []uintptr
	frames []StackFrame
}

// Error returns the underlying error's message.
func (err *Error) Error() string {
	return err.Err.Error()
}

// Return the wrapped error (implements api for As function).
func (err *Error) Unwrap() error {
	return err.Err
}

func (err *Error) Is(target error) bool {
	if e, ok := target.(*Error); ok {
		return baseErrors.Is(err.Err, e.Err)
	}
	return baseErrors.Is(err.Err, target)
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

// Callers satisfies the bugsnag ErrorWithCallerS() interface
// so that the stack can be read out.
func (err *Error) Callers() []uintptr {
	return err.stack
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
