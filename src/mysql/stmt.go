package mysql

import (
	"database/sql/driver"
)

type Stmt struct {
	id uint32

	// COM_STMT_PREPARE
	query string

	// COM_STMT_PREPARE response
	columnCount  uint16
	paramCount   uint16
	warningCount uint16
	// TODO: where to use the following received column definitions?
	paramDefs  []*columnDefinition
	columnDefs []*columnDefinition

	// COM_STMT_EXECUTE
	flags              uint8
	iterationCount     uint32
	newParamsBoundFlag uint8
	paramValue         []interface{}
	paramValueLength   int // simple optimization, length of values all the parameters
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
