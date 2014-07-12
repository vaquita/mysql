package mysql

import (
	"database/sql/driver"
	"encoding/binary"
	"errors"
	"fmt"
	"time"
)

// protocol packet header size
const (
	packetHeaderSize = 4
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

const (
	clientCapabilityFlags = (clientLongPassword |
		clientLongFlag |
		clientTransactions |
		clientProtocol41 |
		//clientSecureConnection |
		clientMultiResults |
		clientPluginAuth)
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

//<!-- protocol packet reader/writer -->

// readPacket reads the next protocol packet from the network, verifies the
// packet sequence ID and returns the payload.
func (c *Conn) readPacket() ([]byte, error) {
	var err error

	// first read the packet header
	hBuf := make([]byte, packetHeaderSize)
	if _, err = c.n.read(hBuf); err != nil {
		return nil, err
	}

	// payload length
	payloadLength := getUint24(hBuf[0:3])

	// read and verify packet sequence ID
	if c.sequenceId != uint8(hBuf[3]) {
		return nil, errors.New("mysql: packets out of order")
	}
	// increment the packet sequence ID
	c.sequenceId++

	// finally, read the payload
	pBuf := make([]byte, payloadLength)
	if _, err = c.n.read(pBuf); err != nil {
		return nil, err
	}

	return pBuf, nil
}

// writePacket accepts the protocol packet to be written, populates the header
// and writes it to the network.
func (c *Conn) writePacket(b []byte) error {
	var err error

	// populate the packet header
	putUint24(b[0:3], uint32(len(b)-4)) // payload length
	b[3] = c.sequenceId                 // packet sequence ID

	// write it to the network
	if _, err = c.n.write(b); err != nil {
		return err
	}

	// finally, increment the packet sequence ID
	c.sequenceId++

	return nil
}

//<!-- generic response packets -->

const (
	okPacket                 = 0x00
	errPacket                = 0xff
	eofPacket                = 0xfe
	localInfileRequestPacket = 0xfb
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

//<!-- connection phase packets -->

// parseGreetingPacket parses handshake initialization packet received from
// the server.
func (c *Conn) parseGreetingPacket(b []byte) {
	var (
		off, n                       int
		authData                     []byte // authentiication plugin data
		authDataLength               int
		authDataOff_1, authDataOff_2 int
	)

	off++                                                 // [0a] protocol version
	c.serverVersion, n = getNullTerminatedString(b[off:]) // server version (null-terminated)
	off += n

	c.connectionId = binary.LittleEndian.Uint32(b[off : off+4]) // connection ID
	off += 4

	// auth-plugin-data-part-1 (8 bytes) : note the offset & length
	authDataOff_1 = off
	authDataLength = 8
	off += 8

	off++ // [00] filler

	// capacity flags (lower 2 bytes)
	c.serverCapabilityFlags = uint32(binary.LittleEndian.Uint16(b[off : off+2]))
	off += 2

	if len(b) > off {
		c.serverCharacterSet = uint8(b[off])
		off++

		c.statusFlags = binary.LittleEndian.Uint16(b[off : off+2]) // status flags
		off += 2
		// capacity flags (upper 2 bytes)
		c.serverCapabilityFlags = uint32(binary.LittleEndian.Uint16(b[off:off+2]) << 2)
		off += 2

		if (c.serverCapabilityFlags & clientPluginAuth) != 0 {
			// update the auth plugin data length
			authDataLength = int(b[off])
			off++
		} else {
			off++ // [00]
		}

		off += 10 // reserved (all [00])

		if (c.serverCapabilityFlags & clientSecureConnection) != 0 {
			// auth-plugin-data-part-2 : note the offset & update
			// the length (max(13, authDataLength- 8)
			if (authDataLength - 8) > 13 {
				authDataLength = 13 + 8
			}
			authDataOff_2 = off
			off += (authDataLength - 8)
		}
		authData = make([]byte, authDataLength)
		copy(authData[0:8], b[authDataOff_1:authDataOff_1+8])
		if authDataLength > 8 {
			copy(authData[8:], b[authDataOff_2:authDataOff_2+(authDataLength-8)])
		}

		c.authPluginData = authData

		if (c.serverCapabilityFlags & clientPluginAuth) != 0 {
			// auth-plugin name (null-terminated)
			c.authPluginName, n = getNullTerminatedString(b[off:])
			off += n
		}
	}
}

// createHandshakeResponsePacket generates the handshake response packet.
func (c *Conn) createHandshakeResponsePacket() []byte {
	var (
		off        int
		authData   []byte // auth response data
		authLength int    // auth response data length
	)

	authData = c.authResponseData()
	authLength = len(authData)

	b := make([]byte, packetHeaderSize+
		c.handshakeResponsePayloadLength(authLength))
	off += 4 // placeholder for protocol packet header

	// client capability flags
	binary.LittleEndian.PutUint32(b[off:off+4], clientCapabilityFlags)
	off += 4

	// max packaet size
	binary.LittleEndian.PutUint32(b[off:off+4], c.maxPacketSize)
	off += 4

	// client character set
	b[off] = byte(c.clientCharacterSet) // client character set
	off++

	off += 23 // reserved (all [0])

	off += putNullTerminatedString(b[off:], c.p.username)

	if authLength > 0 {
		if (c.serverCapabilityFlags & clientPluginAuthLenencClientData) != 0 {
			off += putLenencString(b, string(authData))
		} else if (c.serverCapabilityFlags & clientSecureConnection) != 0 {
			b[off] = byte(authLength)
			off++
			off += copy(b[off:], authData)
		} else {
			off += putNullTerminatedString(b[off:], string(authData))
		}
	}

	if (c.serverCapabilityFlags & clientConnectWithDb) != 0 {
		off += putNullTerminatedString(b[off:], c.p.schema)
	}

	if (c.serverCapabilityFlags & clientPluginAuth) != 0 {
		off += putNullTerminatedString(b[off:], c.authPluginName)
	}

	if (c.serverCapabilityFlags & clientConnectAttrs) != 0 {
		// TODO: handle connection attributes
	}

	return b
}

// handshakeResponsePayloadLength returns the payload length of the handshake
// response packet
func (c *Conn) handshakeResponsePayloadLength(authLength int) (length int) {
	length += (4 + 4 + 1 + 23)
	length += (len(c.p.username) + 1) // null-terminated username
	length += authLength

	if (c.serverCapabilityFlags & clientConnectWithDb) != 0 {
		length += (len(c.p.schema) + 1) // null-terminated schema name
	}

	if (c.serverCapabilityFlags & clientPluginAuth) != 0 {
		length += (len(c.authPluginName) + 1) // null-terminated authentication plugin name
	}

	if (c.serverCapabilityFlags & clientConnectAttrs) != 0 {
		// TODO: handle connection attributes
	}
	return
}

//<!-- command phase packets -->

// createComQuit generates the COM_QUIT packet.
func createComQuit() (b []byte) {
	var off int
	payloadLength := 1 // comQuit

	b = make([]byte, packetHeaderSize+payloadLength)
	off += 4 // placeholder for protocol packet header
	b[off] = comQuit
	return
}

// createComInitDb generates the COM_INIT_DB packet.
func createComInitDb(schema string) []byte {
	var off int

	payloadLength := 1 + // comInitDb
		len(schema) // length of schema name

	b := make([]byte, packetHeaderSize+payloadLength)
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

	b := make([]byte, packetHeaderSize+payloadLength)
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

	b := make([]byte, packetHeaderSize+payloadLength)
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

	b := make([]byte, packetHeaderSize+payloadLength)
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

	b := make([]byte, packetHeaderSize+payloadLength)
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

	b := make([]byte, packetHeaderSize+payloadLength)
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

	b := make([]byte, packetHeaderSize+payloadLength)
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

	b := make([]byte, packetHeaderSize+payloadLength)
	off += 4 // placeholder for protocol packet header

	b[off] = comStatistics
	off++

	return b
}

// createComProcessInfo generates COM_PROCESS_INFO packet.
func createComProcessInfo() []byte {
	var off int

	payloadLength := 1 // comProcessInfo

	b := make([]byte, packetHeaderSize+payloadLength)
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
	col.characterSet = binary.LittleEndian.Uint16(b[off : off+2])
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
	var (
		err    error
		_query string
		_args  []interface{}
	)

	// reset the protocol packet sequence number
	c.sequenceId = 0

	if len(args) > 0 {
		// TODO: find a better way to perform the []driver.Value ->
		// []interface{} conversion
		_args = make([]interface{}, len(args))
		for i := 0; i < len(args); i++ {
			switch v := args[i].(type) {
			default:
				_args[i] = v
			}
		}
		_query = fmt.Sprintf(query, _args...)
	} else {
		_query = query
	}

	// send COM_QUERY to the server
	if err = c.writePacket(createComQuery(_query)); err != nil {
		return nil, err
	}

	return c.handleExecResponse()
}

// handleQuery handles COM_QUERY and related packets for Conn's Query()
func (c *Conn) handleQuery(query string, args []driver.Value) (driver.Rows, error) {
	var (
		err    error
		_query string
		_args  []interface{}
	)

	// reset the protocol packet sequence number
	c.sequenceId = 0

	if len(args) > 0 {
		// TODO: find a better way to perform the []driver.Value ->
		// []interface{} conversion
		_args = make([]interface{}, len(args))
		for i := 0; i < len(args); i++ {
			switch v := args[i].(type) {
			default:
				_args[i] = v
			}
		}
		_query = fmt.Sprintf(query, _args...)
	} else {
		_query = query
	}

	// send COM_QUERY to the server
	if err = c.writePacket(createComQuery(_query)); err != nil {
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
	case errPacket:
		c.parseErrPacket(b)
		return nil, &c.e
	case okPacket:
		c.parseOkPacket(b)
		break
	default:
		// TODO: handle error
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
	case errPacket:
		c.parseErrPacket(b)
		return nil, &c.e
	case localInfileRequestPacket: // local infile request
		// TODO: add support for local infile request
	default: // result set
		// get the column count (TODO: columnCount: uint16 or uint64 // ??)
		columnCount, _ := getLenencInt(b)
		return c.handleResultSet(uint16(columnCount))
	}

	// control shouldn't reach here
	return nil, nil
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
	c.sequenceId = 0

	return c.writePacket(createComQuit())
}
