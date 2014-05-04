package mysql

import (
	"database/sql/driver"
)

type Conn struct {
}

func (c *Conn) Prepare(query string) (driver.Stmt, error) {
	stmt := &Stmt{}
	return stmt, nil
}

func (c *Conn) Close() error {
	return nil
}

func (c *Conn) Begin() (driver.Tx, error) {
	tx := &Tx{}
	return tx, nil
}

func (c *Conn) Exec(query string, args []driver.Value) (driver.Result, error) {
	result := &Result{}
	return result, nil
}

func (c *Conn) Query(query string, args []driver.Value) (driver.Rows, error) {
	rows := &Rows{}
	return rows, nil
}
