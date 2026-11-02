package job

import (
	"errors"
	"fmt"
)

// Sentinel errors. Callers compare with errors.Is, which also works
// when the error has been wrapped with fmt.Errorf("...: %w", err).
var (
	ErrNotFound          = errors.New("job not found")
	ErrInvalidTransition = errors.New("invalid status transition")
	ErrLeaseLost         = errors.New("job lease lost") // another worker took over, or the job was changed
)

// ValidationError describes one bad input field. The API turns it into
// a 400 response; callers find it with errors.As.
type ValidationError struct {
	Field   string
	Message string
}

func (e *ValidationError) Error() string {
	return fmt.Sprintf("invalid %s: %s", e.Field, e.Message)
}

// permanentError marks a handler failure that retrying cannot fix,
// for example a payload that is missing a required field.
type permanentError struct{ err error }

func (e *permanentError) Error() string { return "permanent: " + e.err.Error() }
func (e *permanentError) Unwrap() error { return e.err }

// Permanent wraps err so the worker marks the job dead immediately
// instead of retrying it.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return &permanentError{err: err}
}

// IsPermanent reports whether err (or anything it wraps) was created by Permanent.
func IsPermanent(err error) bool {
	var p *permanentError
	return errors.As(err, &p)
}
