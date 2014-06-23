package mysql

import (
	"database/sql/driver"
)

type Conn struct {
	// connection properties
	p properties
	n _net

	// OK packet
	affectedRows uint64
	lastInsertId uint64
	statusFlags  uint16
	warnings     uint16

	// ERR packet
	e Error

	// handshake initialization packet
	serverVersion         string
	connectionId          uint32
	serverCapabilityFlags uint32
	serverCharacterSet    uint8
	authPluginDataLength  uint8
	authPluginData        string
	authPluginName        string

	// handshake response packet
	clientCapabilityFlags uint32
	maxPacketSize         uint32
	clientCharacterSet    uint8
	authResponseData      string

	sequenceId uint8 // packet sequence number

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
