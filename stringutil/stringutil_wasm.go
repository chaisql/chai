package errorutil

import (
	"errors"
	"strconv"
	"strings"
)

func Errorf(msg string, args ...string) error {
	return errorF(msg, args...)
}
