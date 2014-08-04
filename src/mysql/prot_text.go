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

// server commands
const (
	comSleep = iota
	comQuit
	comInitDb
	comQuery
	comFieldList
	comCreateDb
	comDropDb
	comRefresh
	comShutdown
	comStatistics
	comProcessInfo
	comConnect
	comProcessKill
	comDebug
	comPing
	comTime
	comDelayedInsert
	comChangeUser
	comBinlogDump
	comTableDump
	comConnectOuT
	comRegisterSlave
	comStmtPrepare
	comStmtExecute
	comStmtSendLongData
	comStmtClose
	comStmtReset
	comSetOption
	comStmtFetch
	comDaemon
	comEnd // must always be last
)

// client/server capability flags
const (
	clientLongPassword = 1 << iota
	clientFoundRows
	clientLongFlag
	clientConnectWithDb
	clientNoSchema
	clientCompress
	clientODBC
	clientLocalFiles
	clientIgnoreSpace
	clientProtocol41
	clientInteractive
	clientSSL
	clientIgnoreSIGPIPE
	clientTransactions
	clientReserved
	clientSecureConnection
	clientMultiStatements
	clientMultiResults
	clientPSMultiResults
	clientPluginAuth
	clientConnectAttrs
	clientPluginAuthLenencClientData
	clientCanHandleExpiredPasswords
	clientSessionTrack
	_ // unassigned, 1 << 24
	_
	_
	_
	_
	clientProgress // 1 << 29
	clientSSLVerifyServerCert
	clientRememberOptions
)

// server status flags
const (
	serverStatusInTrans = 1 << iota
	serverStatusAutocommit
	_ // unassigned, 4
	serverMoreResultsExists
	serverStatusNoGoodIndexUsed
	serverStatusNoIndexUsed
	serverStatusCursorExists
	serverStatusLastRowSent
	serverStatusDbDropped
	serverStatusNoBackshashEscapes
	serverStatusMetadataChanged
	serverQueryWasSlow
	serverPSOutParams
	serverStatusInTransReadonly
	serverSessionStateChanged
)

//<!-- generic response packets -->

const (
	okPacket        = 0x00
	errPacket       = 0xff
	eofPacket       = 0xfe
	infileReqPacket = 0xfb
)

// parseOkPacket parses the OK packet received from the server.
func (c *Conn) parseOkPacket(b []byte) {
	var off, n int

	off++ // [00] the OK header (= okPacket)
	c.affectedRows, n = getLenencInt(b[off:])
	off += n
	c.lastInsertId, n = getLenencInt(b[off:])
	off += n

	c.statusFlags = binary.LittleEndian.Uint16(b[off : off+2])
	off += 2
	c.warnings = binary.LittleEndian.Uint16(b[off : off+2])
	off += 2
	// TODO : read rest of the fields
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
func (c *Conn) parseEOFPacket(b []byte) {
	var off int

	off++ // [fe] the EOF header (= eofPacket)
	// TODO: reset warning count
	c.warnings += binary.LittleEndian.Uint16(b[off : off+2])
	off += 2
	c.statusFlags = binary.LittleEndian.Uint16(b[off : off+2])
}

//<!-- command phase packets -->

// createComQuit generates the COM_QUIT packet.
func createComQuit() (b []byte) {
	var off int
	payloadLength := 1 // comQuit

	b = make([]byte, 4+payloadLength)
	off += 4 // placeholder for protocol packet header
	b[off] = comQuit
	return
}

// createComInitDb generates the COM_INIT_DB packet.
func createComInitDb(schema string) []byte {
	var off int

	payloadLength := 1 + // comInitDb
		len(schema) // length of schema name

	b := make([]byte, 4+payloadLength)
	off += 4 // placeholder for protocol packet header

	b[off] = comInitDb
	off++

	off += copy(b[off:], schema)
	return b
}

// createComQuery generates the COM_QUERY packet.
func createComQuery(query string) []byte {
	var off int

	payloadLength := 1 + // comQuery
		len(query) // length of query

	b := make([]byte, 4+payloadLength)
	off += 4 // placeholder for protocol packet header

	b[off] = comQuery
	off++

	off += copy(b[off:], query)
	return b
}

// createComFieldList generates the COM_FILED_LIST packet.
func createComFieldList(table, fieldWildcard string) []byte {
	var off int

	payloadLength := 1 + // comFieldList
		len(table) + // length of table name
		1 + // NULL
		len(fieldWildcard) // length of field wildcard

	b := make([]byte, 4+payloadLength)
	off += 4 // placeholder for protocol packet header

	b[off] = comFieldList
	off++

	off += copy(b[off:], table)
	off++

	off += copy(b[off:], fieldWildcard)

	return b
}

// createComCreateDb generates the COM_CREATE_DB packet.
func createComCreateDb(schema string) []byte {
	var off int

	payloadLength := 1 + // comCreateDb
		len(schema) // length of schema name

	b := make([]byte, 4+payloadLength)
	off += 4 // placeholder for protocol packet header

	b[off] = comCreateDb
	off++

	off += copy(b[off:], schema)
	return b
}

// createComDropDb generates the COM_DROP_DB packet.
func createComDropDb(schema string) []byte {
	var off int

	payloadLength := 1 + // comDropDb
		len(schema) // length of schema name

	b := make([]byte, 4+payloadLength)
	off += 4 // placeholder for protocol packet header

	b[off] = comDropDb
	off++

	off += copy(b[off:], schema)
	return b
}

type MyRefreshOption uint8

const (
	RefreshGrant   MyRefreshOption = 0x01
	RefreshLog     MyRefreshOption = 0x02
	RefreshTables  MyRefreshOption = 0x04
	RefreshHosts   MyRefreshOption = 0x08
	RefreshStatus  MyRefreshOption = 0x10
	RefreshSlave   MyRefreshOption = 0x20
	RefreshThreads MyRefreshOption = 0x40
	RefreshMaster  MyRefreshOption = 0x80
)

// createComRefresh generates COM_REFRESH packet.
func createComRefresh(subCommand uint8) []byte {
	var off int

	payloadLength := 1 + // comRefresh
		1 // subCommand length

	b := make([]byte, 4+payloadLength)
	off += 4 // placeholder for protocol packet header

	b[off] = comRefresh
	off++
	b[off] = subCommand
	off++
	return b
}

type MyShutdownLevel uint8

const (
	ShutdownDefault             MyShutdownLevel = 0x00
	ShutdownWaitConnections     MyShutdownLevel = 0x01
	ShutdownWaitTransactions    MyShutdownLevel = 0x02
	ShutdownWaitUpdates         MyShutdownLevel = 0x08
	ShutdownWaitAllBuffers      MyShutdownLevel = 0x10
	ShutdownWaitCriticalBuffers MyShutdownLevel = 0x11
	KillQuery                   MyShutdownLevel = 0xfe
	KillConnections             MyShutdownLevel = 0xff
)

// createComShutdown generates COM_SHUTDOWN packet.
func createComShutdown(level MyShutdownLevel) []byte {
	var off int

	payloadLength := 1 + // comShutdown
		1 // shutdown level length

	b := make([]byte, 4+payloadLength)
	off += 4 // placeholder for protocol packet header

	b[off] = comShutdown
	off++
	b[off] = byte(level)
	off++
	return b
}

// createComStatistics generates COM_STATISTICS packet.
func createComStatistics() []byte {
	var off int

	payloadLength := 1 // comStatistics

	b := make([]byte, 4+payloadLength)
	off += 4 // placeholder for protocol packet header

	b[off] = comStatistics
	off++

	return b
}

// createComProcessInfo generates COM_PROCESS_INFO packet.
func createComProcessInfo() []byte {
	var off int

	payloadLength := 1 // comProcessInfo

	b := make([]byte, 4+payloadLength)
	off += 4 // placeholder for protocol packet header

	b[off] = comProcessInfo
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
		err error
		b   []byte
	)

	if b, err = c.readPacket(); err != nil {
		return nil, err
	}

	switch b[0] {

	case errPacket: // expected
		// handle err packet
		c.parseErrPacket(b)
		return nil, &c.e

	case okPacket: // expected
		// parse Ok packet and break
		c.parseOkPacket(b)
		break

	case infileReqPacket: // expected
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
	return res, nil
}

func (c *Conn) handleQueryResponse() (*Rows, error) {
	var (
		err error
		b   []byte
	)

	if b, err = c.readPacket(); err != nil {
		return nil, err
	}

	switch b[0] {
	case errPacket: // expected
		// handle err packet
		c.parseErrPacket(b)
		return nil, &c.e

	case okPacket: // unexpected!
		// the command resulted in a Result (anti-pattern ?); but
		// since it succeeded we handle it and return nil.
		c.parseOkPacket(b)
		return nil, nil

	case infileReqPacket: // unexpected!
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
		err  error
		b    []byte
		done bool
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
		c.parseEOFPacket(b)
	}

	// read resultset row packets (each containing rs.columnCount values),
	// until EOF packet.
	for !done {
		if b, err = c.readPacket(); err != nil {
			return nil, err
		}

		switch b[0] {
		case eofPacket:
			done = true
		case errPacket:
			c.parseErrPacket(b)
			return nil, &c.e
		default: // result set row
			rs.rows = append(rs.rows,
				c.handleResultSetRow(b, rs))
		}
	}
	return rs, nil
}

func (c *Conn) handleResultSetRow(b []byte, rs *Rows) *row {
	var (
		v      NullString
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
		b             []byte
		err, savedErr error
	)

	// do not skip on error to avoid "packets out of order"
	if b, err = createInfileDataPacket(filename); err != nil {
		savedErr = err
		goto L
	} else if err = c.writePacket(b); err != nil {
		savedErr = err
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
	case errPacket:
		// handle err packet
		c.parseErrPacket(b)
		return &c.e

	case okPacket:
		// parse Ok packet
		c.parseOkPacket(b)

	default:
		// TODO: handle error
	}
	return savedErr
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
		return nil, err
	}
	defer f.Close()

	if fi, err = f.Stat(); err != nil {
		return nil, err
	}

	b := make([]byte, 4+fi.Size())

	if _, err = f.Read(b[4:]); err != nil {
		return nil, err
	}

	return b, nil
}

// createEmptyPacket generates an empty packet.
func createEmptyPacket() []byte {
	return make([]byte, 4)
}
