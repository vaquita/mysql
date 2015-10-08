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
	var (
		err error
		p   properties
	)

	// parse the dsn
	if err = p.parseUrl(dsn); err != nil {
		return nil, err
	}

	if p.scheme != "mysql" {
		return nil, myError(ErrScheme, p.scheme)
	}

	return open(p)
}
