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

	// handshake initialization packet (from server)
	serverVersion         string
	connectionId          uint32
	serverCapabilityFlags uint32
	serverCharacterSet    uint8
	authPluginDataLength  uint8
	authPluginData        string
	authPluginName        string

	// handshake response packet (from client)
	clientCapabilityFlags uint32
	maxPacketSize         uint32
	clientCharacterSet    uint8

	sequenceId uint8 // packet sequence number
}

func (c *Conn) Prepare(query string) (driver.Stmt, error) {
	return c.handleStmtPrepare(query)
}

func (c *Conn) Close() error {
	return c.handleComQuit()
}

func (c *Conn) Begin() (driver.Tx, error) {
	tx := &Tx{}
	return tx, nil
}

func (c *Conn) Exec(query string, args []driver.Value) (driver.Result, error) {
	return c.handleExec(query, args)
}

func (c *Conn) Query(query string, args []driver.Value) (driver.Rows, error) {
	return c.handleQuery(query, args)
}
