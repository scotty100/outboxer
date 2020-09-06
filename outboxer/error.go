package outboxer

import (
	"fmt"
	"strings"
)

type ConstError string
func (err ConstError) Error() string {
	return string(err)
}
func (err ConstError) Is(target error) bool {
	ts := target.Error()
	es := string(err)
	return ts == es || strings.HasPrefix(ts, es+": ")
}
func (err ConstError) Wrap(inner error) error {
	return WrapError{Msg: string(err), Err: inner}
}

type WrapError struct {
	Err error
	Msg string
}
func (err WrapError) Error() string {
	if err.Err != nil {
		return fmt.Sprintf("%s: %v", err.Msg, err.Err)
	}
	return err.Msg
}
func (err WrapError) Unwrap() error {
	return err.Err
}
func (err WrapError) Is(target error) bool {
	return ConstError(err.Msg).Is(target)
}

