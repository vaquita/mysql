package mysql

import (
	"database/sql"
	"database/sql/driver"
)

type Driver struct {
}

// init registers the driver
func init() {
	sql.Register("mysql", &Driver{})
}

func (d Driver) Open(dsn string) (driver.Conn, error) {
	var err error

	c := &Conn{}
	c.rw = &defaultReadWriter{}

	// parse the dsn
	if err = c.p.parseUrl(dsn); err != nil {
		return nil, err
	}

	// open a connection with the server
	if c.conn, err = dial(c.p.address, c.p.socket); err != nil {
		return nil, err
	}

	// perform handshake
	if err = c.handshake(); err != nil {
		return nil, err
	}
	return c, nil
}
