package schema

import (
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/santhosh-tekuri/jsonschema/v6"
	"golang.org/x/text/message"
)

var printer = message.NewPrinter(message.MatchLanguage("en"))

type Violation struct {
	Path   string `json:"path"`
	Detail string `json:"detail"`
}

type ValidationError struct {
	Schema     string      `json:"schema"`
	Violations []Violation `json:"violations"`
}

func (e *ValidationError) Error() string {
	details := make([]string, 0, len(e.Violations))
	for _, violation := range e.Violations {
		if violation.Path == "" {
			details = append(details, violation.Detail)
			continue
		}

		details = append(details, violation.Path+": "+violation.Detail)
	}

	return fmt.Sprintf("schema %s rejected the payload: %s", e.Schema, strings.Join(details, "; "))
}

func AsValidationError(err error) (*ValidationError, bool) {
	var validation *ValidationError
	ok := errors.As(err, &validation)

	return validation, ok
}

func collectViolations(err *jsonschema.ValidationError) []Violation {
	violations := make([]Violation, 0, len(err.Causes)+1)

	if len(err.Causes) == 0 {
		return append(violations, Violation{
			Path:   instancePath(err.InstanceLocation),
			Detail: err.ErrorKind.LocalizedString(printer),
		})
	}

	for _, cause := range err.Causes {
		violations = append(violations, collectViolations(cause)...)
	}

	sort.Slice(violations, func(i, j int) bool {
		if violations[i].Path == violations[j].Path {
			return violations[i].Detail < violations[j].Detail
		}

		return violations[i].Path < violations[j].Path
	})

	return violations
}

func instancePath(location []string) string {
	if len(location) == 0 {
		return ""
	}

	return "/" + strings.Join(location, "/")
}
