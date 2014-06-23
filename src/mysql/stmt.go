package mysql

import (
	"database/sql/driver"
)

type Stmt struct {
	// COM_STMT_PREPARE
	query string

	// COM_STMT_PREPARE response
	id           uint32
	columnCount  uint16
	paramCount   uint16
	warningCount uint16
	paramDefs    []*columnDefinition
	columnDefs   []*columnDefinition

	// COM_STMT_EXECUTE
	flags              uint8
	iterationCount     uint32
	nullBitmap         []byte
	newParamsBoundFlag uint8
	paramType          []uint16
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
