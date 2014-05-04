package mysql

import (
	"database/sql/driver"
)

type Driver struct {
}

func (d *Driver) Open(dsn string) (driver.Conn, error) {
	conn := &Conn{}
	return conn, nil
}
