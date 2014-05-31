package mysql

import (
	"database/sql/driver"
)

type Conn struct {
	user     string
	password string
	host     string
	socket   string
	port     uint16
	schema   string

	// OK packet
	affectedRows uint64
	lastInsertId uint64
	statusFlags  uint16
	warnings     uint16

	// ERR packet
	errorCode    uint16
	sqlState     string
	errorMessage string

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
