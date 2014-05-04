package mysql

import (
	"database/sql/driver"
)

type Stmt struct {
}

func (s *Stmt) Close() error {
	return nil
}

func (s *Stmt) NumInput() int {
	return 0
}

func (s *Stmt) Exec(args []driver.Value) (driver.Result, error) {
	result := &Result{}
	return result, nil
}

func (s *Stmt) Query(arg []driver.Value) (driver.Rows, error) {
	rows := &Rows{}
	return rows, nil
}
