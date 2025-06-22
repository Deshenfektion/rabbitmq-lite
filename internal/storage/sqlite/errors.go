package sqlite

import (
	"errors"
	"strings"

	sqlitedriver "modernc.org/sqlite"
)

const (
	codeConstraintUnique  = 2067
	codeConstraintPrimary = 1555
)

func isUniqueViolation(err error) bool {
	var driverErr *sqlitedriver.Error
	if errors.As(err, &driverErr) {
		switch driverErr.Code() {
		case codeConstraintUnique, codeConstraintPrimary:
			return true
		}
	}

	return strings.Contains(err.Error(), "UNIQUE constraint failed")
}
