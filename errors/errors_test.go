package errors

import (
	"testing"

	ee "github.com/genjidb/genji/internal/errors"
)

func TestIsNotFoundError(t *testing.T) {
	if !IsNotFoundError(NotFoundError{Name: "foo"}) {
		t.Fail()
	}
	if !IsNotFoundError(ee.Wrap(NotFoundError{Name: "foo"})) {
		t.Fail()
	}
}

func TestIsAlreadyExistsError(t *testing.T) {
	if !IsAlreadyExistsError(AlreadyExistsError{Name: "foo"}) {
		t.Fail()
	}
	if !IsAlreadyExistsError(ee.Wrap(AlreadyExistsError{Name: "foo"})) {
		t.Fail()
	}
}
