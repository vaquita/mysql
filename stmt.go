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
	paramDefs  []*columnDefinition
	columnDefs []*columnDefinition

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
