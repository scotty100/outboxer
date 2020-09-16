package outboxer

import (
	"fmt"
	"strings"
)

//https://medium.com/@smyrman/writing-constant-errors-with-go-1-13-10c4191617

// ConstError allows you to define constant errors
// they can be validated against (is) and wrap runtime errors
type ConstError string

func (err ConstError) Error() string {
	return string(err)
}

// determine if the error is the defined ConstError
func (err ConstError) Is(target error) bool {
	ts := target.Error()
	es := string(err)
	return ts == es || strings.HasPrefix(ts, es+": ")
}

// wrap a runtime error in the constant error
func (err ConstError) Wrap(inner error) error {
	return wrapError{Msg: string(err), Err: inner}
}

// wrappable error struct
type wrapError struct {
	Err error
	Msg string
}

// error string output from wrapped error hierarchy
func (err wrapError) Error() string {
	if err.Err != nil {
		return fmt.Sprintf("%s: %v", err.Msg, err.Err)
	}
	return err.Msg
}

// get wrapped error
func (err wrapError) Unwrap() error {
	return err.Err
}

// enable error
func (err wrapError) Is(target error) bool {
	return ConstError(err.Msg).Is(target)
}
