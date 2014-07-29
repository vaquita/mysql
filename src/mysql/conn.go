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
