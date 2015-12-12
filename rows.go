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
