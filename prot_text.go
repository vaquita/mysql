/*
  Copyright (C) 2015 Nirbhay Choubey

  This library is free software; you can redistribute it and/or
  modify it under the terms of the GNU Lesser General Public
  License as published by the Free Software Foundation; either
  version 2.1 of the License, or (at your option) any later version.

  This library is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
  Lesser General Public License for more details.

  You should have received a copy of the GNU Lesser General Public
  License along with this library; if not, write to the Free Software
  Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301
  USA
*/

package mysql

import (
	"database/sql/driver"
	"encoding/binary"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"
)

// server commands (unexported)
const (
	_ = iota // _COM_SLEEP
	_COM_QUIT
	_COM_INIT_DB
	_COM_QUERY
	_COM_FIELD_LIST
	_COM_CREATE_DB
	_COM_DROP_DB
	_COM_REFRESH
	_COM_SHUTDOWN
	_COM_STATISTICS
	_COM_PROCESS_INFO
	_COM_CONNECT
	_ // _COM_PROCESS_KILL
	_ //_COM_DEBUG
	_COM_PING
	_ // _COM_TIME
	_ //_COM_DELAYED_INSERT
	_COM_CHANGE_USER
	_COM_BINLOG_DUMP
	_COM_TABLE_DUMP
	_ // _COM_CONNECT_OUT
	_COM_REGISTER_SLAVE
	_COM_STMT_PREPARE
	_COM_STMT_EXECUTE
	_COM_STMT_SEND_LONG_DATA
	_COM_STMT_CLOSE
	_COM_STMT_RESET
	_COM_SET_OPTION
	_COM_STMT_FETCH
	_        // _COM_DAEMON
	_COM_END // must always be last
)

// client/server capability flags (unexported)
const (
	_CLIENT_LONG_PASSWORD = 1 << iota
	_CLIENT_FOUND_ROWS
	_CLIENT_LONG_FLAG
	_CLIENT_CONNECT_WITH_DB
	_CLIENT_NO_SCHEMA
	_CLIENT_COMPRESS
	_CLIENT_ODBC
	_CLIENT_LOCAL_FILES
	_CLIENT_IGNORE_SPACE
	_CLIENT_PROTOCOL41
	_CLIENT_INTERACTIVE
	_CLIENT_SSL
	_CLIENT_IGNORE_SIGPIPE
	_CLIENT_TRANSACTIONS
	_CLIENT_RESERVED
	_CLIENT_SECURE_CONNECTION
	_CLIENT_MULTI_STATEMENTS
	_CLIENT_MULTI_RESULTS
	_CLIENT_PS_MULTI_RESULTS
	_CLIENT_PLUGIN_AUTH
	_CLIENT_CONNECT_ATTRS
	_CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA
	_CLIENT_CAN_HANDLE_EXPIRED_PASSWORDS
	_CLIENT_SESSION_TRACK
	_ // unassigned, 1 << 24
	_
	_
	_
	_
	_CLIENT_PROGRESS // 1 << 29
	_CLIENT_SSL_VERIFY_SERVER_CERT
	_CLIENT_REMEMBER_OPTIONS
)

// server status flags (unexported)
const (
	_SERVER_STATUS_IN_TRANS = 1 << iota
	_SERVER_STATUS_AUTOCOMMIT
	_ // unassigned, 4
	_SERVER_MORE_RESULTS_EXISTS
	_SERVER_STATUS_NO_GOOD_INDEX_USED
	_SERVER_STATUS_NO_INDEX_USED
	_SERVER_STATUS_CURSOR_EXISTS
	_SERVER_STATUS_LAST_ROW_SENT
	_SERVER_STATUS_DB_DROPPED
	_SERVER_STATUS_NO_BACKSHASH_ESCAPES
	_SERVER_STATUS_METADATA_CHANGED
	_SERVER_QUERY_WAS_SLOW
	_SERVER_PS_OUT_PARAMS
	_SERVER_STATUS_IN_TRANS_READONLY
	_SERVER_SESSION_STATE_CHANGED
)

// generic response packets (unexported)
const (
	_PACKET_OK         = 0x00
	_PACKET_ERR        = 0xff
	_PACKET_EOF        = 0xfe
	_PACKET_INFILE_REQ = 0xfb
)

// parseOkPacket parses the OK packet received from the server.
func (c *Conn) parseOkPacket(b []byte) bool {
	var off, n int

	off++ // [00] the OK header (= _PACKET_OK)
	c.affectedRows, n = getLenencInt(b[off:])
	off += n
	c.lastInsertId, n = getLenencInt(b[off:])
	off += n

	c.statusFlags = binary.LittleEndian.Uint16(b[off : off+2])
	off += 2
	c.warnings = binary.LittleEndian.Uint16(b[off : off+2])
	off += 2
	// TODO : read rest of the fields

	return c.reportWarnings()
}

// parseErrPacket parses the ERR packet received from the server.
func (c *Conn) parseErrPacket(b []byte) {
	var off int

	off++ // [ff] the ERR header (= errPacket)
	c.e.code = binary.LittleEndian.Uint16(b[off : off+2])
	off += 2
	off++ // '#' the sql-state marker
	c.e.sqlState = string(b[off : off+5])
	off += 5
	c.e.message = string(b[off:])
	c.e.when = time.Now()
}

// parseEOFPacket parses the EOF packet received from the server.
func (c *Conn) parseEOFPacket(b []byte) bool {
	var off int

	off++ // [fe] the EOF header (= _PACKET_EOF)
	// TODO: reset warning count
	c.warnings += binary.LittleEndian.Uint16(b[off : off+2])
	off += 2
	c.statusFlags = binary.LittleEndian.Uint16(b[off : off+2])

	return c.reportWarnings()
}

func (c *Conn) reportWarnings() bool {
	if c.p.reportWarnings && c.warnings > 0 {
		c.e.code = 0
		c.e.sqlState = "01000"
		c.e.message = "last command resulted in warning(s)"
		c.e.warnings = c.warnings
		c.e.when = time.Now()
		return true // warnings reported
	}
	return false
}

//<!-- command phase packets -->

// createComQuit generates the COM_QUIT packet.
func createComQuit() (b []byte) {
	var off int
	payloadLength := 1 // _COM_QUIT

	b = make([]byte, 4+payloadLength)
	off += 4 // placeholder for protocol packet header
	b[off] = _COM_QUIT
	return
}

// createComInitDb generates the COM_INIT_DB packet.
func createComInitDb(schema string) []byte {
	var off int

	payloadLength := 1 + // _COM_INIT_DB
		len(schema) // length of schema name

	b := make([]byte, 4+payloadLength)
	off += 4 // placeholder for protocol packet header

	b[off] = _COM_INIT_DB
	off++

	off += copy(b[off:], schema)
	return b
}

// createComQuery generates the COM_QUERY packet.
func createComQuery(query string) []byte {
	var off int

	payloadLength := 1 + // _COM_QUERY
		len(query) // length of query

	b := make([]byte, 4+payloadLength)
	off += 4 // placeholder for protocol packet header

	b[off] = _COM_QUERY
	off++

	off += copy(b[off:], query)
	return b
}

// createComFieldList generates the COM_FILED_LIST packet.
func createComFieldList(table, fieldWildcard string) []byte {
	var off int

	payloadLength := 1 + // _COM_FIELD_LIST
		len(table) + // length of table name
		1 + // NULL
		len(fieldWildcard) // length of field wildcard

	b := make([]byte, 4+payloadLength)
	off += 4 // placeholder for protocol packet header

	b[off] = _COM_FIELD_LIST
	off++

	off += copy(b[off:], table)
	off++

	off += copy(b[off:], fieldWildcard)

	return b
}

// createComCreateDb generates the COM_CREATE_DB packet.
func createComCreateDb(schema string) []byte {
	var off int

	payloadLength := 1 + // _COM_CREATE_DB
		len(schema) // length of schema name

	b := make([]byte, 4+payloadLength)
	off += 4 // placeholder for protocol packet header

	b[off] = _COM_CREATE_DB
	off++

	off += copy(b[off:], schema)
	return b
}

// createComDropDb generates the COM_DROP_DB packet.
func createComDropDb(schema string) []byte {
	var off int

	payloadLength := 1 + // _COM_DROP_DB
		len(schema) // length of schema name

	b := make([]byte, 4+payloadLength)
	off += 4 // placeholder for protocol packet header

	b[off] = _COM_DROP_DB
	off++

	off += copy(b[off:], schema)
	return b
}

// refresh flags (exported)
const (
	REFRESH_GRANT   = 0x01
	REFRESH_LOG     = 0x02
	REFRESH_TABLES  = 0x04
	REFRESH_HOSTS   = 0x08
	REFRESH_STATUS  = 0x10
	REFRESH_SLAVE   = 0x20
	REFRESH_THREADS = 0x40
	REFRESH_MASTER  = 0x80
)

// createComRefresh generates COM_REFRESH packet.
func createComRefresh(subCommand uint8) []byte {
	var off int

	payloadLength := 1 + // _COM_REFRESH
		1 // subCommand length

	b := make([]byte, 4+payloadLength)
	off += 4 // placeholder for protocol packet header

	b[off] = _COM_REFRESH
	off++
	b[off] = subCommand
	off++
	return b
}

type MyShutdownLevel uint8

// shutdown flags (exported)
const (
	SHUTDOWN_DEFAULT               MyShutdownLevel = 0x00
	SHUTDOWN_WAIT_CONNECTIONS      MyShutdownLevel = 0x01
	SHUTDOWN_WAIT_TRANSACTIONS     MyShutdownLevel = 0x02
	SHUTDOWN_WAIT__UPDATES         MyShutdownLevel = 0x08
	SHUTDOWN_WAIT_ALL_BUFFERS      MyShutdownLevel = 0x10
	SHUTDOWN_WAIT_CRITICAL_BUFFERS MyShutdownLevel = 0x11
	SHUTDOWN_KILL_QUERY            MyShutdownLevel = 0xfe
	SHUTDOWN_KILL_CONNECTIONS      MyShutdownLevel = 0xff
)

// createComShutdown generates COM_SHUTDOWN packet.
func createComShutdown(level MyShutdownLevel) []byte {
	var off int

	payloadLength := 1 + // _COM_SHUTDOWN
		1 // shutdown level length

	b := make([]byte, 4+payloadLength)
	off += 4 // placeholder for protocol packet header

	b[off] = _COM_SHUTDOWN
	off++
	b[off] = byte(level)
	off++
	return b
}

// createComStatistics generates COM_STATISTICS packet.
func createComStatistics() []byte {
	var off int

	payloadLength := 1 // _COM_STATISTICS

	b := make([]byte, 4+payloadLength)
	off += 4 // placeholder for protocol packet header

	b[off] = _COM_STATISTICS
	off++

	return b
}

// createComProcessInfo generates COM_PROCESS_INFO packet.
func createComProcessInfo() []byte {
	var off int

	payloadLength := 1 // _COM_PROCESS_INFO

	b := make([]byte, 4+payloadLength)
	off += 4 // placeholder for protocol packet header

	b[off] = _COM_PROCESS_INFO
	return b
}

// parseColumnDefinitionPacket parses the column (field) definition packet.
func parseColumnDefinitionPacket(b []byte, isComFieldList bool) *columnDefinition {
	var off, n int

	// alloc a new columnDefinition object
	col := new(columnDefinition)

	col.catalog, n = getLenencString(b[off:])
	off += n
	col.schema, n = getLenencString(b[off:])
	off += n
	col.table, n = getLenencString(b[off:])
	off += n
	col.orgTable, n = getLenencString(b[off:])
	off += n
	col.name, n = getLenencString(b[off:])
	off += n
	col.orgName, n = getLenencString(b[off:])
	off += n
	col.fixedLenFieldLength, n = getLenencInt(b[off:])
	off += n
	col.charset = binary.LittleEndian.Uint16(b[off : off+2])
	off += 2
	col.columnLength = binary.LittleEndian.Uint32(b[off : off+4])
	off += 4
	col.columnType = uint8(b[off])
	off++
	col.flags = binary.LittleEndian.Uint16(b[off : off+2])
	off += 2
	col.decimals = uint8(b[off])
	off++

	off += 2 //filler [00] [00]

	if isComFieldList == true {
		col.defaultValues, _ = getLenencString(b)
	}

	return col
}

// handleExec handles COM_QUERY and related packets for Conn's Exec()
func (c *Conn) handleExec(query string, args []driver.Value) (driver.Result, error) {
	// reset the protocol packet sequence number
	c.resetSeqno()

	// send COM_QUERY to the server
	if err := c.writePacket(createComQuery(replacePlaceholders(query, args))); err != nil {
		return nil, err
	}

	return c.handleExecResponse()
}

// handleQuery handles COM_QUERY and related packets for Conn's Query()
func (c *Conn) handleQuery(query string, args []driver.Value) (driver.Rows, error) {
	// reset the protocol packet sequence number
	c.resetSeqno()

	// send COM_QUERY to the server
	if err := c.writePacket(createComQuery(replacePlaceholders(query, args))); err != nil {
		return nil, err
	}

	return c.handleQueryResponse()
}

func (c *Conn) handleExecResponse() (*Result, error) {
	var (
		err  error
		b    []byte
		warn bool
	)

	if b, err = c.readPacket(); err != nil {
		return nil, err
	}

	switch b[0] {

	case _PACKET_ERR: // expected
		// handle err packet
		c.parseErrPacket(b)
		return nil, &c.e

	case _PACKET_OK: // expected
		// parse Ok packet and break
		warn = c.parseOkPacket(b)

	case _PACKET_INFILE_REQ: // expected
		// local infile request; handle it
		if err = c.handleInfileRequest(string(b[1:])); err != nil {
			return nil, err
		}
	default: // unexpected
		// the command resulted in Rows (anti-pattern ?); but since it
		// succeeded, we handle it and return nil
		columnCount, _ := getLenencInt(b)
		_, err = c.handleResultSet(uint16(columnCount)) // Rows ignored!
		return nil, err
	}

	res := new(Result)
	res.lastInsertId = int64(c.lastInsertId)
	res.rowsAffected = int64(c.affectedRows)

	if warn {
		// command resulted in warning(s), return results and error
		return res, &c.e
	}

	return res, nil
}

func (c *Conn) handleQueryResponse() (*Rows, error) {
	var (
		err  error
		b    []byte
		warn bool
	)

	if b, err = c.readPacket(); err != nil {
		return nil, err
	}

	switch b[0] {
	case _PACKET_ERR: // expected
		// handle err packet
		c.parseErrPacket(b)
		return nil, &c.e

	case _PACKET_OK: // unexpected!
		// the command resulted in a Result (anti-pattern ?); but
		// since it succeeded we handle it and return nil.
		warn = c.parseOkPacket(b)

		if warn {
			return nil, &c.e
		}

		return nil, nil

	case _PACKET_INFILE_REQ: // unexpected!
		// local infile request; handle it and return nil
		if err = c.handleInfileRequest(string(b[1:])); err != nil {
			return nil, err
		}
		return nil, nil

	default: // expected
		// break and handle result set
		break
	}

	// handle result set
	columnCount, _ := getLenencInt(b)
	return c.handleResultSet(uint16(columnCount))
}

func (c *Conn) handleResultSet(columnCount uint16) (*Rows, error) {
	var (
		err        error
		b          []byte
		done, warn bool
	)

	rs := new(Rows)
	rs.columnDefs = make([]*columnDefinition, 0)
	rs.rows = make([]*row, 0)
	rs.columnCount = columnCount

	// read column definition packets
	for i := uint16(0); i < rs.columnCount; i++ {
		if b, err = c.readPacket(); err != nil {
			return nil, err
		} else {
			rs.columnDefs = append(rs.columnDefs,
				parseColumnDefinitionPacket(b, false))
		}
	}

	// read EOF packet
	if b, err = c.readPacket(); err != nil {
		return nil, err
	} else {
		warn = c.parseEOFPacket(b)
	}

	// read resultset row packets (each containing rs.columnCount values),
	// until EOF packet.
	for !done {
		if b, err = c.readPacket(); err != nil {
			return nil, err
		}

		switch b[0] {
		case _PACKET_EOF:
			done = true
		case _PACKET_ERR:
			c.parseErrPacket(b)
			return nil, &c.e
		default: // result set row
			rs.rows = append(rs.rows,
				c.handleResultSetRow(b, rs))
		}
	}
	if warn {
		// command resulted in warning(s), return results and error
		return rs, &c.e
	}
	return rs, nil
}

func (c *Conn) handleResultSetRow(b []byte, rs *Rows) *row {
	var (
		v      nullString
		off, n int
	)

	columnCount := rs.columnCount
	r := new(row)
	r.columns = make([]interface{}, 0, columnCount)

	for i := uint16(0); i < columnCount; i++ {
		v, n = getLenencString(b[off:])
		if v.valid == true {
			r.columns = append(r.columns, v.value)
		} else {
			r.columns = append(r.columns, nil)
		}
		off += n
	}
	return r
}

func (c *Conn) handleQuit() error {
	// reset the protocol packet sequence number
	c.resetSeqno()

	return c.writePacket(createComQuit())
}

// stringify converts the given argument of arbitrary type to string. 'quote'
// decides whether to quote (single-quote) the give resulting string.
func stringify(d interface{}, quote bool) string {
	switch v := d.(type) {
	case string:
		if quote {
			return "'" + v + "'"
		}
		return v
	case []byte:
		s := string(v)
		if quote {
			return "'" + s + "'"
		}
		return s
	case bool:
		if v {
			return "TRUE"
		} else {
			return "FALSE"
		}
	case time.Time:
		t := fmt.Sprintf("%d-%d-%d %d:%d:%d", v.Year(), int(v.Month()), v.Day(), v.Hour(), v.Minute(), v.Second())
		if quote {
			return strconv.Quote(t)
		}
		return t
	case nil:
		return "NULL"
	}

	rv := reflect.ValueOf(d)
	switch rv.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return strconv.FormatInt(rv.Int(), 10)
	case reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return strconv.FormatUint(rv.Uint(), 10)
	case reflect.Float32:
		return strconv.FormatFloat(rv.Float(), 'f', -1, 32)
	case reflect.Float64:
		return strconv.FormatFloat(rv.Float(), 'f', -1, 64)
	default:
		// TODO: unsupported type?
	}
	return fmt.Sprintf("%v", d)
}

// replacePlaceholders replaces all ?'s with the stringified arguments.
func replacePlaceholders(query string, args []driver.Value) string {
	if len(args) == 0 {
		return query
	}

	s := strings.Split(query, "?")
	final := make([]string, 0)

	for i, arg := range args {
		final = append(final, s[i])
		final = append(final, stringify(arg, true))
	}
	final = append(final, s[len(s)-1])
	return strings.Join(final, "")
}

func (c *Conn) handleInfileRequest(filename string) error {
	var (
		b              []byte
		err, savedErr  error
		errSaved, warn bool
	)

	// do not skip on error to avoid "packets out of order"
	if b, err = createInfileDataPacket(filename); err != nil {
		savedErr = err
		errSaved = true
		goto L
	} else if err = c.writePacket(b); err != nil {
		savedErr = err
		errSaved = true
		goto L
	}

L:
	// send an empty packet
	if err = c.writePacket(createEmptyPacket()); err != nil {
		return err
	}

	// read ok/err packet from the server
	if b, err = c.readPacket(); err != nil {
		return err
	}

	switch b[0] {
	case _PACKET_ERR:
		// handle err packet
		c.parseErrPacket(b)
		return &c.e

	case _PACKET_OK:
		// parse Ok packet
		warn = c.parseOkPacket(b)

	default:
		return myError(ErrInvalidPacket)
	}

	if errSaved {
		return savedErr
	}

	if warn {
		return &c.e
	}

	return nil

}

// createInfileDataPacket generates a packet containing contents of the
// requested local file
func createInfileDataPacket(filename string) ([]byte, error) {
	var (
		f   *os.File
		fi  os.FileInfo
		err error
	)

	if f, err = os.Open(filename); err != nil {
		return nil, myError(ErrFile, err)
	}
	defer f.Close()

	if fi, err = f.Stat(); err != nil {
		return nil, myError(ErrFile, err)
	}

	b := make([]byte, 4+fi.Size())

	if _, err = f.Read(b[4:]); err != nil {
		return nil, myError(ErrFile, err)
	}

	return b, nil
}

// createEmptyPacket generates an empty packet.
func createEmptyPacket() []byte {
	return make([]byte, 4)
}
