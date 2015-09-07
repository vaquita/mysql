package mysql

import (
	"database/sql"
	"database/sql/driver"
	"errors"
)

type Driver struct {
}

// init registers the driver
func init() {
	sql.Register("mysql", &Driver{})
}

func (d Driver) Open(dsn string) (driver.Conn, error) {
	var (
		err error
		p   properties
	)

	// parse the dsn
	if err = p.parseUrl(dsn); err != nil {
		return nil, err
	}

	if p.scheme != "mysql" {
		return nil, errors.New("mysql: invalid url scheme")
	}

	return open(p)
}
