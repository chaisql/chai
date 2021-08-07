package testutil

import (
	"testing"

	"github.com/genjidb/genji/internal/errors"
)

func ErrorIs(t *testing.T, err error, target error) {
	t.Helper()
	ErrorIsf(t, err, target, "Expected error to be %v but got %v instead", target, err)
}

func ErrorIsf(t *testing.T, err error, target error, str string, args ...interface{}) {
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

func NoErrorf(t *testing.T, err error, str string, args ...interface{}) {
	t.Helper()

	if err == nil {
		return
	}
	t.Logf(str, args...)
	t.Logf("%#v", err)
	if e, ok := err.(*errors.Error); ok {
		t.Logf("Stacktrace:\n%s", string(e.Stack()))
	}
	t.FailNow()
}

func NoError(t *testing.T, err error) {
	t.Helper()

	NoErrorf(t, err, "Expected error to be nil but got %v instead", err)
}
