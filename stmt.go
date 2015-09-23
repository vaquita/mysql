package mysql

import (
	"database/sql/driver"
)

type Stmt struct {
	id uint32
	c  *Conn

	// COM_STMT_PREPARE
	query string

	// COM_STMT_PREPARE response
	columnCount uint16
	paramCount  uint16
	warnings    uint16
	// TODO: where to use the following received column definitions?
	paramDefs  []*columnDefinition
	columnDefs []*columnDefinition

	// COM_STMT_EXECUTE
	flags              uint8
	iterationCount     uint32
	newParamsBoundFlag uint8
}

func (s *Stmt) Close() error {
	return s.handleClose()
}

func (s *Stmt) NumInput() int {
	return int(s.paramCount)
}

func (s *Stmt) Exec(args []driver.Value) (driver.Result, error) {
	return s.handleExec(args)
}

func (s *Stmt) Query(args []driver.Value) (driver.Rows, error) {
	return s.handleQuery(args)
}

func (s *Stmt) ColumnConverter(idx int) driver.ValueConverter {
	return defaultParameterConverter
}
