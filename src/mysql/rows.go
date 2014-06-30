package mysql

import (
	"database/sql/driver"
)

type Rows struct {
	columnCount uint16
	columnDefs  []*columnDefinition
	rows        []*row
}

type columnDefinition struct {
	catalog             NullString
	schema              NullString
	table               NullString
	orgTable            NullString
	name                NullString
	orgName             NullString
	fixedLenFieldLength uint64
	characterSet        uint16
	columnLength        uint32
	columnType          uint8
	flags               uint16
	decimals            uint8
	defaultValues       string
}

type row struct {
	columns []interface{}
}

func (r *Rows) Columns() []string {
	return nil
}

func (r *Rows) Close() error {
	return nil
}

func (r *Rows) Next(dest []driver.Value) error {
	return nil
}
