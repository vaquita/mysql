package mysql

import (
	"database/sql/driver"
)

type Driver struct {
}

func (d *Driver) Open(dsn string) (driver.Conn, error) {
	var err error

	c := &Conn{}

	// parse the dsn
	if err = c.p.parseUrl(dsn); err != nil {
		return nil, err
	}

	// open a connection with the server
	if err = c.n.dial(c.p.address, c.p.socket); err != nil {
		return nil, err
	}

	// perform handshake
	if err = c.handshake(); err != nil {
		return nil, err
	}

	return c, nil
}
