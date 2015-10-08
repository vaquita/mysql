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
	"database/sql/driver"
)

func (c *Conn) Prepare(query string) (driver.Stmt, error) {
	return c.handleStmtPrepare(query)
}

func (c *Conn) Close() error {
	return c.handleQuit()
}

func (c *Conn) Begin() (driver.Tx, error) {
	if _, err := c.handleExec("START TRANSACTION", nil); err != nil {
		return nil, err
	}
	tx := new(Tx)
	tx.c = c
	return tx, nil
}

func (c *Conn) Exec(query string, args []driver.Value) (driver.Result, error) {
	return c.handleExec(query, args)
}

func (c *Conn) Query(query string, args []driver.Value) (driver.Rows, error) {
	return c.handleQuery(query, args)
}
