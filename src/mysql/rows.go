package mysql

import (
	"database/sql/driver"
)

type Rows struct {
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
