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
