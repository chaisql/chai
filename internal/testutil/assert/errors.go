package assert

import (
	"testing"

	"github.com/genjidb/genji/internal/errors"
)

func Error(t testing.TB, err error) {
	t.Helper()
	if err != nil {
		return
	}
	t.Log("Expected error to be present, but got nil instead")
	t.FailNow()
}

func Errorf(t testing.TB, err error, str string, args ...interface{}) {
	t.Helper()

	if err != nil {
		return
	}
	t.Logf(str, args...)
	t.FailNow()
}

func ErrorIs(t testing.TB, err error, target error) {
	t.Helper()
	ErrorIsf(t, err, target, "Expected error to be %v but got %v instead", target, err)
}

func ErrorIsf(t testing.TB, err error, target error, str string, args ...interface{}) {
	t.Helper()

	if errors.Is(err, target) {
		return
	}
	t.Logf(str, args...)
	if e, ok := err.(*errors.Error); ok {
		t.Logf("Stacktrace:\n%s", string(e.Stack()))
	}
	t.FailNow()
}

func NoErrorf(t testing.TB, err error, str string, args ...interface{}) {
	t.Helper()

	if err == nil {
		return
	}
	t.Logf(str, args...)
	if e, ok := err.(*errors.Error); ok {
		t.Logf("Stacktrace:\n%s", string(e.Stack()))
	}
	t.FailNow()
}

func NoError(t testing.TB, err error) {
	t.Helper()

	NoErrorf(t, err, "Expected error to be nil but got %q instead", err)
}
