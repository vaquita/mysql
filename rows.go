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
	"io"
)

type Rows struct {
	columnCount uint16
	columnDefs  []*columnDefinition
	rows        []*row

	// iterator-related
	pos    uint64
	closed bool
}

type columnDefinition struct {
	catalog             nullString
	schema              nullString
	table               nullString
	orgTable            nullString
	name                nullString
	orgName             nullString
	fixedLenFieldLength uint64
	charset             uint16
	columnLength        uint32
	columnType          uint8
	flags               uint16
	decimals            uint8
	defaultValues       nullString
}

type row struct {
	columns []interface{}
}

func (r *Rows) Columns() []string {
	columns := make([]string, 0, r.columnCount)
	for i := 0; i < int(r.columnCount); i++ {
		columns = append(columns, r.columnDefs[i].name.value)
	}
	return columns
}

func (r *Rows) Close() error {
	// reset the iterator position and mark it as closed
	r.pos = 0
	r.closed = true
	return nil
}

func (r *Rows) Next(dest []driver.Value) error {
	if r.closed == true {
		return myError(ErrCursor)
	}

	if r.pos >= uint64(len(r.rows)) {
		return io.EOF
	}

	for i, v := range r.rows[r.pos].columns {
		dest[i] = v
	}
	r.pos++
	return nil
}
