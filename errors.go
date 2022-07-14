package genji

import (
	"github.com/cockroachdb/errors"
	"github.com/genjidb/genji/internal/database"
	errs "github.com/genjidb/genji/internal/errors"
)

// IsNotFoundError determines if the given error is a NotFoundError.
// NotFoundError is returned when the requested table, index, document or sequence
// doesn't exist.
var IsNotFoundError = errs.IsNotFoundError

// IsAlreadyExistsError determines if the error is returned as a result of
// a conflict when attempting to create a table, an index, a document or a sequence
// with a name that is already used by another resource.
func IsAlreadyExistsError(err error) bool {
	if errs.IsAlreadyExistsError(err) {
		return true
	}

	for err != nil {
		if cerr, ok := err.(*database.ConstraintViolationError); ok {
			switch cerr.Constraint {
			case "UNIQUE", "PRIMARY KEY":
				return true
			default:
				return false
			}
		}

		err = errors.Unwrap(err)
	}

	return false
}
