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
	serverVersion      string
	connectionId       uint32
	serverCapabilities uint32
	serverCharset      uint8
	authPluginData     []byte
	authPluginName     string

	// handshake response packet (from client)
	clientCharset uint8

	seqno uint8 // packet sequence number
}

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
