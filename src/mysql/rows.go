package mysql

import (
	"database/sql/driver"
)

type Rows struct {
	columnCount uint64
	columnDefs  []*columnDefinition
	rows []*row
}

type columnDefinition struct {
	catalog             string
	schema              string
	table               string
	orgTable            string
	name                string
	orgName             string
	fixedLenFieldLength uint64
	characterSet        uint16
	columnLength        uint32
	columnType          uint8
	flags               uint16
	decimals            uint8
	defaultValuesLength uint64
	defaultValues       string
}

type row struct {
  cols []interface{}
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
