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
	"database/sql/driver"
)

type Stmt struct {
	id uint32
	c  *Conn

	// COM_STMT_PREPARE
	query string

	// COM_STMT_PREPARE response
	columnCount uint16
	paramCount  uint16
	warnings    uint16
	// TODO: where to use the following received column definitions?
	paramDefs  []*ColumnDefinition
	columnDefs []*ColumnDefinition

	// COM_STMT_EXECUTE
	flags              uint8
	iterationCount     uint32
	newParamsBoundFlag uint8
}

func (s *Stmt) Close() error {
	return s.handleClose()
}

func (s *Stmt) NumInput() int {
	return int(s.paramCount)
}

func (s *Stmt) Exec(args []driver.Value) (driver.Result, error) {
	return s.handleExec(args)
}

func (s *Stmt) Query(args []driver.Value) (driver.Rows, error) {
	return s.handleQuery(args)
}

func (s *Stmt) ColumnConverter(idx int) driver.ValueConverter {
	return defaultParameterConverter
}
