/*
  Copyright (C) 2015 Nirbhay Choubey

  This library is free software; you can redistribute it and/or
  modify it under the terms of the GNU Lesser General Public
  License as published by the Free Software Foundation; either
  version 2.1 of the License, or (at your option) any later version.

  This library is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
  Lesser General Public License for more details.

  You should have received a copy of the GNU Lesser General Public
  License along with this library; if not, write to the Free Software
  Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301
  USA
*/

package mysql

import (
	"fmt"
	"time"
)

type Error struct {
	code     uint16
	sqlState string
	message  string
	warnings uint16
	when     time.Time
}

// client error codes
const (
	ErrWarning = 0
	ErrUnknown = 9000 + iota
	ErrConnection
	ErrRead
	ErrWrite
	ErrSSLSupport
	ErrSSLConnection
	ErrCompressionSupport
	ErrCompression
	ErrInvalidType
	ErrInvalidDSN
	ErrInvalidProperty
	ErrScheme
	ErrCursor
	ErrFile
	ErrInvalidPacket
)

var errFormat = map[uint16]string{
	ErrWarning:            "Execution of last statement resulted in warning(s)",
	ErrUnknown:            "Unknown error",
	ErrConnection:         "Can't connect to the server (%s)",
	ErrRead:               "Can't read data from connection (%s)",
	ErrWrite:              "Can't write data to connection (%s)",
	ErrSSLSupport:         "Server does not support SSL connection",
	ErrSSLConnection:      "Can't establish SSL connection with the server (%s)",
	ErrCompressionSupport: "Server does not support packet compression",
	ErrCompression:        "Compression error (%s)",
	ErrInvalidType:        "Invalid type (%s)",
	ErrInvalidDSN:         "Can't parse data source name (%s)",
	ErrInvalidProperty:    "Invalid value for property '%s' (%s)",
	ErrScheme:             "Unsupported scheme '%s'",
	ErrCursor:             "Cursor is closed",
	ErrFile:               "File operation failed (%s)",
	ErrInvalidPacket:      "Invalid/unexpected packet received",
}

func myError(code uint16, a ...interface{}) *Error {
	return &Error{code: code,
		message: fmt.Sprintf(errFormat[code], a...),
		when:    time.Now()}
}

// Error returns the formatted error message. (also required by Go's error
// interface)
func (e *Error) Error() string {
	if e.code == ErrWarning || e.code >= ErrUnknown {
		// client error
		return fmt.Sprintf("[mysql] %d : %s", e.code, e.message)
	}
	// server error
	return fmt.Sprintf("[mysqld] %d (%s): %s", e.code, e.sqlState, e.message)
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

func (e *Error) Warnings() uint16 {
	return e.warnings
}
