/*
  The MIT License (MIT)

  Copyright (c) 2015 Nirbhay Choubey

  Permission is hereby granted, free of charge, to any person obtaining a copy
  of this software and associated documentation files (the "Software"), to deal
  in the Software without restriction, including without limitation the rights
  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
  copies of the Software, and to permit persons to whom the Software is
  furnished to do so, subject to the following conditions:

  The above copyright notice and this permission notice shall be included in all
  copies or substantial portions of the Software.

  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
  SOFTWARE.
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
	ErrInvalidPropertyValue
	ErrNetPacketTooLarge
	ErrNetPacketsOutOfOrder
)

var errFormat = map[uint16]string{
	ErrWarning:              "Execution of last statement resulted in warning(s)",
	ErrUnknown:              "Unknown error",
	ErrConnection:           "Can't connect to the server (%s)",
	ErrRead:                 "Can't read data from connection (%s)",
	ErrWrite:                "Can't write data to connection (%s)",
	ErrSSLSupport:           "Server does not support SSL connection",
	ErrSSLConnection:        "Can't establish SSL connection with the server (%s)",
	ErrCompressionSupport:   "Server does not support packet compression",
	ErrCompression:          "Compression error (%s)",
	ErrInvalidType:          "Invalid type (%s)",
	ErrInvalidDSN:           "Can't parse data source name (%s)",
	ErrInvalidProperty:      "Invalid property '%s'",
	ErrScheme:               "Unsupported scheme '%s'",
	ErrCursor:               "Cursor is closed",
	ErrFile:                 "File operation failed (%s)",
	ErrInvalidPacket:        "Invalid/unexpected packet received",
	ErrInvalidPropertyValue: "Invalid value for property '%s' (%v)",
	ErrNetPacketTooLarge:    "Got a packet bigger than MaxAllowedPacket",
	ErrNetPacketsOutOfOrder: "Got packets out of order",
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
		return fmt.Sprintf("mysql: [%d] %s", e.code, e.message)
	}
	// server error
	return fmt.Sprintf("mysqld: [%d] (%s) %s", e.code, e.sqlState, e.message)
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
