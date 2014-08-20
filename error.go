package mysql

import (
	"fmt"
	"time"
)

type Error struct {
	code     uint16
	sqlState string
	message  string
	when     time.Time
}

// Error returns the formatted error message. (also required by Go' error
// interface)
func (e *Error) Error() string {
	return fmt.Sprintf("mysqld: %d (%s): %s", e.code, e.sqlState, e.message)
}

// Code returns the error number.
func (e *Error) Code() uint16 {
	return e.code
}

// SqlState returns the SQL STATE
func (e *Error) SqlState() string {
	return e.sqlState
}

// Message returns the error message.
func (e *Error) Message() string {
	return e.message
}

// When returns time when error occurred.
func (e *Error) When() time.Time {
	return e.when
}
