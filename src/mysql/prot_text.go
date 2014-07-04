package mysql

import (
	"bytes"
	"encoding/binary"
	"errors"
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
	payloadLength := getUint32_3(hBuf[0:3])

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
	putUint32_3(b[0:3], uint32(len(b))) // payload length
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
func (c *Conn) parseOkPacket(b *bytes.Buffer) {
	b.Next(1) // [00] the OK header (= okPacket)
	c.affectedRows = getLenencInteger(b)
	c.lastInsertId = getLenencInteger(b)

	c.statusFlags = binary.LittleEndian.Uint16(b.Next(2))
	c.warnings = binary.LittleEndian.Uint16(b.Next(2))
	// TODO : read rest of the fields
}

// parseErrPacket parses the ERR packet received from the server.
func (c *Conn) parseErrPacket(b *bytes.Buffer) {
	b.Next(1) // [ff] the ERR header (= errPacket)

	c.e.code = binary.LittleEndian.Uint16(b.Next(2))
	b.Next(1) // '#' the sql-state marker
	c.e.sqlState = string(b.Next(5))
	c.e.message = string(b.Next(b.Len()))
	c.e.when = time.Now()
}

// parseEOFPacket parses the EOF packet received from the server.
func (c *Conn) parseEOFPacket(b *bytes.Buffer) {
	b.Next(1) // [fe] the EOF header (= eofPacket)
	// TODO: reset warning count
	c.warnings += binary.LittleEndian.Uint16(b.Next(2))
	c.statusFlags = binary.LittleEndian.Uint16(b.Next(2))
}

//<!-- connection phase packets -->

// parseHandshakePacket parses handshake initialization packet received from
// the server.
func (c *Conn) parseHandshakePacket(b *bytes.Buffer) {
	var authPluginDataBuf bytes.Buffer

	b.Next(1)                                              // [0a] protocol version
	c.serverVersion, _ = b.ReadString(0)                   // server version (null-terminated)
	c.connectionId = binary.LittleEndian.Uint32(b.Next(4)) // connection ID

	// auth-plugin-data-part-1 (8 bytes)
	authPluginDataBuf.WriteString(string(b.Next(8)))

	b.Next(1) // [00] filler
	// capacity flags (lower 2 bytes)
	c.serverCapabilityFlags = uint32(binary.LittleEndian.Uint16(b.Next(2)))

	if b.Len() > 0 {
		c.serverCharacterSet = uint8(b.Next(1)[0])
		c.statusFlags = binary.LittleEndian.Uint16(b.Next(2)) // status flags
		// capacity flags (upper 2 bytes)
		c.serverCapabilityFlags = uint32(binary.LittleEndian.Uint16(b.Next(2)) << 2)

		if (c.serverCapabilityFlags & clientPluginAuth) != 0 {
			c.authPluginDataLength = uint8(b.Next(1)[0])
		} else {
			b.Next(1) // [00]
		}

		b.Next(10) // reserved (all [00])

		if (c.serverCapabilityFlags & clientSecureConnection) != 0 {
			// auth-plugin-data-part-1 (8 bytes)
			// max(13, c.authPluginDataLength - 8)
			authPluginDataBuf.WriteString(string(b.Next(13)))
		}
		c.authPluginData = authPluginDataBuf.String()

		if (c.serverCapabilityFlags & clientPluginAuth) != 0 {
			// auth-plugin name (null-terminated)
			c.authPluginName, _ = b.ReadString(0)
		}
	}
}

// createHandshakeResponsePacket generates the handshake response packet.
func createHandshakeResponsePacket(c *Conn) *bytes.Buffer {
	payloadLength := c.handshakeResponsePacketLength()

	b := bytes.NewBuffer(make([]byte, packetHeaderSize+payloadLength))
	b.Next(4) // placeholder for protocol packet header

	// client capability flags
	binary.LittleEndian.PutUint32(b.Next(4), c.clientCapabilityFlags)
	// max packaet size
	binary.LittleEndian.PutUint32(b.Next(4), c.maxPacketSize)
	b.WriteByte(c.clientCharacterSet) // client character set
	b.Next(23)                        // reserved (all [0])

	putNullTerminatedString(b, c.p.username)

	// auth response data
	if data := c.authResponseData(); len(data) > 0 {
		if (c.serverCapabilityFlags & clientPluginAuthLenencClientData) != 0 {
			putLenencString(b, string(data))
		} else if (c.serverCapabilityFlags & clientSecureConnection) != 0 {
			b.WriteByte(byte(len(data)))
			b.Write(data)
		} else {
			putNullTerminatedString(b, string(data))
		}
	}

	if (c.serverCapabilityFlags & clientConnectWithDb) != 0 {
		putNullTerminatedString(b, c.p.schema)
	}

	if (c.serverCapabilityFlags & clientPluginAuth) != 0 {
		putNullTerminatedString(b, c.authPluginName)
	}

	if (c.serverCapabilityFlags & clientConnectAttrs) != 0 {
		// TODO: handle connection attributes
	}
	return b
}

func (c *Conn) handshakeResponsePacketLength() int {
	return 0
}

//<!-- command phase packets -->

// createComQuit generates the COM_QUIT packet.
func createComQuit() (*bytes.Buffer, error) {
	payloadLength := 1 // comQuit

	b := bytes.NewBuffer(make([]byte, packetHeaderSize+payloadLength))
	b.Next(4) // placeholder for protocol packet header

	b.WriteByte(comQuit)

	return b, nil
}

// createComInitDb generates the COM_INIT_DB packet.
func createComInitDb(schema string) (*bytes.Buffer, error) {
	var err error

	payloadLength := 1 + // comInitDb
		len(schema) // length of schema name

	b := bytes.NewBuffer(make([]byte, packetHeaderSize+payloadLength))
	b.Next(4) // placeholder for protocol packet header

	b.WriteByte(comInitDb)
	if _, err = b.WriteString(schema); err != nil {
		return nil, err
	}

	return b, nil
}

// createComQuery generates the COM_QUERY packet.
func createComQuery(query string) (*bytes.Buffer, error) {
	var err error

	payloadLength := 1 + // comQuery
		len(query) // length of query

	b := bytes.NewBuffer(make([]byte, packetHeaderSize+payloadLength))
	b.Next(4) // placeholder for protocol packet header

	b.WriteByte(comQuery)
	if _, err = b.WriteString(query); err != nil {
		return nil, err
	}

	return b, nil
}

// createComFieldList generates the COM_FILED_LIST packet.
func createComFieldList(table, fieldWildcard string) (*bytes.Buffer, error) {
	var err error

	payloadLength := 1 + // comFieldList
		len(table) + // length of table name
		1 + // NULL
		len(fieldWildcard) // length of field wildcard

	b := bytes.NewBuffer(make([]byte, packetHeaderSize+payloadLength))
	b.Next(4) // placeholder for protocol packet header

	b.WriteByte(comFieldList)
	if _, err = b.WriteString(table); err != nil {
		return nil, err
	}
	b.WriteByte(0)
	if _, err = b.WriteString(fieldWildcard); err != nil {
		return nil, err
	}

	return b, nil
}

// createComCreateDb generates the COM_CREATE_DB packet.
func createComCreateDb(schema string) (*bytes.Buffer, error) {
	var err error

	payloadLength := 1 + // comCreateDb
		len(schema) // length of schema name

	b := bytes.NewBuffer(make([]byte, packetHeaderSize+payloadLength))
	b.Next(4) // placeholder for protocol packet header

	b.WriteByte(comCreateDb)
	if _, err = b.WriteString(schema); err != nil {
		return nil, err
	}

	return b, nil
}

// createComDropDb generates the COM_DROP_DB packet.
func createComDropDb(schema string) (*bytes.Buffer, error) {
	var err error

	payloadLength := 1 + // comDropDb
		len(schema) // length of schema name

	b := bytes.NewBuffer(make([]byte, packetHeaderSize+payloadLength))
	b.Next(4) // placeholder for protocol packet header

	b.WriteByte(comDropDb)
	if _, err = b.WriteString(schema); err != nil {
		return nil, err
	}

	return b, nil
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
func createComRefresh(subCommand uint8) (*bytes.Buffer, error) {
	payloadLength := 1 + // comRefresh
		1 // subCommand length

	b := bytes.NewBuffer(make([]byte, packetHeaderSize+payloadLength))
	b.Next(4) // placeholder for protocol packet header

	b.WriteByte(comRefresh)
	b.WriteByte(subCommand)

	return b, nil
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
func createComShutdown(level MyShutdownLevel) (*bytes.Buffer, error) {
	payloadLength := 1 + // comShutdown
		1 // shutdown level length

	b := bytes.NewBuffer(make([]byte, packetHeaderSize+payloadLength))
	b.Next(4) // placeholder for protocol packet header

	b.WriteByte(comShutdown)
	b.WriteByte(byte(level))

	return b, nil
}

// createComStatistics generates COM_STATISTICS packet.
func createComStatistics() (*bytes.Buffer, error) {
	payloadLength := 1 // comStatistics

	b := bytes.NewBuffer(make([]byte, packetHeaderSize+payloadLength))
	b.Next(4) // placeholder for protocol packet header

	b.WriteByte(comStatistics)

	return b, nil
}

// createComProcessInfo generates COM_PROCESS_INFO packet.
func createComProcessInfo() (*bytes.Buffer, error) {
	payloadLength := 1 // comProcessInfo

	b := bytes.NewBuffer(make([]byte, packetHeaderSize+payloadLength))
	b.Next(4) // placeholder for protocol packet header

	b.WriteByte(comProcessInfo)

	return b, nil
}

// parseColumnDefinitionPacket parses the column (field) definition packet.
func parseColumnDefinitionPacket(b *bytes.Buffer, isComFieldList bool) *columnDefinition {
	// alloc a new columnDefinition object
	col := new(columnDefinition)

	col.catalog = getLenencString(b)
	col.schema = getLenencString(b)
	col.table = getLenencString(b)
	col.orgTable = getLenencString(b)
	col.name = getLenencString(b)
	col.orgName = getLenencString(b)
	col.fixedLenFieldLength = getLenencInteger(b)
	col.characterSet = binary.LittleEndian.Uint16(b.Next(2))
	col.columnLength = binary.LittleEndian.Uint32(b.Next(4))
	col.columnType = uint8(b.Next(1)[0])
	col.flags = binary.LittleEndian.Uint16(b.Next(2))
	col.decimals = uint8(b.Next(1)[0])

	b.Next(2) //filler [00] [00]

	if isComFieldList == true {
		len := getLenencInteger(b)
		col.defaultValues = string(b.Next(int(len)))
	}

	return col
}

func parseResultSetRowPacket(b *bytes.Buffer, columnCount uint64) *row {
	var val NullString

	r := new(row)
	r.columns = make([]interface{}, columnCount)

	for i := uint64(0); i < columnCount; i++ {
		val = getLenencString(b)
		if val.valid == true {
			r.columns = append(r.columns, val.value)
		} else {
			r.columns = append(r.columns, nil)
		}
	}

	return r
}

func (c *Conn) handleResultSetRow(b *bytes.Buffer, rs *Rows) *row {
	r := new(row)
	r.columns = make([]interface{}, 0)

	for i := uint16(0); i < rs.columnCount; i++ {
		r.columns = append(r.columns, getLenencString(b))
	}
	return r
}

func (c *Conn) handleResultSet() (*Rows, error) {
	var (
		err  error
		b    []byte
		done bool
	)

	rs := new(Rows)
	rs.columnDefs = make([]*columnDefinition, 0)
	rs.rows = make([]*row, 0)

	// read the packet containing the column count (stored in length-encoded
	// integer)
	if b, err = c.readPacket(); err != nil {
		return nil, err
	}

	// TODO: columnCount: uint16 or uint64 ??
	rs.columnCount = uint16(getLenencInteger(bytes.NewBuffer(b)))

	// read column definition packets
	for i := uint16(0); i < rs.columnCount; i++ {
		if b, err = c.readPacket(); err != nil {
			return nil, err
		} else {
			rs.columnDefs = append(rs.columnDefs,
				parseColumnDefinitionPacket(bytes.NewBuffer(b), false))
		}
	}

	// read EOF packet
	if b, err = c.readPacket(); err != nil {
		return nil, err
	} else {
		c.parseEOFPacket(bytes.NewBuffer(b))
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
			c.parseErrPacket(bytes.NewBuffer(b))
			return nil, &c.e
		default: // result set row
			rs.rows = append(rs.rows,
				c.handleResultSetRow(bytes.NewBuffer(b), rs))
		}
	}
	return rs, nil
}

func (c *Conn) handleComQueryResponse() (*Rows, error) {
	var (
		err error
		b   []byte
	)

	if b, err = c.readPacket(); err != nil {
		return nil, err
	}

	switch b[0] {
	case errPacket:
		c.parseErrPacket(bytes.NewBuffer(b))
		return nil, &c.e
	case okPacket:
		c.parseOkPacket(bytes.NewBuffer(b))
		return nil, nil
	case localInfileRequestPacket: // local infile request
		// TODO: add support for local infile request
	default: // result set
		return c.handleResultSet()
	}

	// control shouldn't reach here
	return nil, nil
}
