package mysql

import (
	"bytes"
	"encoding/binary"
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
	comSetOptioN
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

// parseOKPacket parses the OK packet received from the server.
func (c *Conn) parseOKPacket(b *bytes.Buffer) {
	b.Next(1) // [00] the OK header
	c.affectedRows = getLenencInteger(b)
	c.lastInsertId = getLenencInteger(b)

	c.statusFlags = binary.LittleEndian.Uint16(b.Next(2))
	c.warnings = binary.LittleEndian.Uint16(b.Next(2))
	// TODO : read rest of fields
}

// parseERRPacket parses the ERR packet received from the server.
func (c *Conn) parseERRPacket(b *bytes.Buffer) {
	b.Next(1) // [ff] the ERR header
	c.errorCode = binary.LittleEndian.Uint16(b.Next(2))
	b.Next(1) // '#' the sql-state marker
	c.sqlState = string(b.Next(5))
	c.errorMessage = string(b.Next(b.Len()))
}

// parseEOFPacket parses the EOF packet received from the server.
func (c *Conn) parseEOFPacket(b *bytes.Buffer) {
	b.Next(1) // [fe] the EOF header
	c.warnings = binary.LittleEndian.Uint16(b.Next(2))
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
func (c *Conn) createHandshakeResponsePacket() *bytes.Buffer {
	payloadLength := c.handshakeResponsePacketLength()

	b := bytes.NewBuffer(make([]byte, packetHeaderSize+payloadLength))
	b.Next(4)

	// client capability flags
	binary.LittleEndian.PutUint32(b.Next(4), c.clientCapabilityFlags)
	// max packaet size
	binary.LittleEndian.PutUint32(b.Next(4), c.maxPacketSize)
	b.WriteByte(c.clientCharacterSet) // client character set
	b.Next(23)                        // reserved (all [0])

	putNullTerminatedString(b, c.user)

	if (c.serverCapabilityFlags & clientPluginAuthLenencClientData) != 0 {
		putLenencString(b, c.authResponseData)
	} else if (c.serverCapabilityFlags & clientSecureConnection) != 0 {
		b.WriteByte(byte(len(c.authResponseData)))
		b.WriteString(c.authResponseData)
	} else {
		putNullTerminatedString(b, c.authResponseData)
	}

	if (c.serverCapabilityFlags & clientConnectWithDb) != 0 {
		putNullTerminatedString(b, c.schema)
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
func (c *Conn) createComQuit() (*bytes.Buffer, error) {
	var err error

	payloadLength := 1 // comQuit

	b := bytes.NewBuffer(make([]byte, packetHeaderSize+payloadLength))
	b.Next(4)

	if err = b.WriteByte(comQuit); err != nil {
		return nil, err
	}

	return b, nil
}

// createComInitDb generates the COM_INIT_DB packet.
func (c *Conn) createComInitDb(schema string) (*bytes.Buffer, error) {
	var err error

	payloadLength := 1 + // comInitDb
		len(schema) // length of schema name

	b := bytes.NewBuffer(make([]byte, packetHeaderSize+payloadLength))
	b.Next(4)

	if err = b.WriteByte(comInitDb); err != nil {
		return nil, err
	}

	if _, err = b.WriteString(schema); err != nil {
		return nil, err
	}

	return b, nil
}

// createComQuery generates the COM_QUERY packet.
func (c *Conn) createComQuery(query string) (*bytes.Buffer, error) {
	var err error

	payloadLength := 1 + // comQuery
		len(query) // length of query

	b := bytes.NewBuffer(make([]byte, packetHeaderSize+payloadLength))
	b.Next(4)

	if err = b.WriteByte(comQuery); err != nil {
		return nil, err
	}

	if _, err = b.WriteString(query); err != nil {
		return nil, err
	}

	return b, nil
}

// createComFieldList generates the COM_FILED_LIST packet.
func (c *Conn) createComFieldList(table, fieldWildcard string) (*bytes.Buffer, error) {
	var err error

	payloadLength := 1 + // comFieldList
		len(table) + // length of table name
		1 + // NULL
		len(fieldWildcard) // length of field wildcard

	b := bytes.NewBuffer(make([]byte, packetHeaderSize+payloadLength))
	b.Next(4)

	if err = b.WriteByte(comFieldList); err != nil {
		return nil, err
	}

	if _, err = b.WriteString(table); err != nil {
		return nil, err
	}

	if err = b.WriteByte(0); err != nil {
		return nil, err
	}

	if _, err = b.WriteString(fieldWildcard); err != nil {
		return nil, err
	}

	return b, nil
}

// createComCreateDb generates the COM_CREATE_DB packet.
func (c *Conn) createComCreateDb(schema string) (*bytes.Buffer, error) {
	var err error

	payloadLength := 1 + // comCreateDb
		len(schema) // length of schema name

	b := bytes.NewBuffer(make([]byte, packetHeaderSize+payloadLength))
	b.Next(4)

	if err = b.WriteByte(comCreateDb); err != nil {
		return nil, err
	}

	if _, err = b.WriteString(schema); err != nil {
		return nil, err
	}

	return b, nil
}

// createComDropDb generate the COM_DROP_DB packet.
func (c *Conn) createComDropDb(schema string) (*bytes.Buffer, error) {
	var err error

	payloadLength := 1 + // comDropDb
		len(schema) // length of schema name

	b := bytes.NewBuffer(make([]byte, packetHeaderSize+payloadLength))
	b.Next(4)

	if err = b.WriteByte(comDropDb); err != nil {
		return nil, err
	}

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
func (c *Conn) createComRefresh(subCommand uint8) (*bytes.Buffer, error) {
	var err error

	payloadLength := 1 + // comRefresh
		1 // subCommand length

	b := bytes.NewBuffer(make([]byte, packetHeaderSize+payloadLength))
	b.Next(4)

	if err = b.WriteByte(comRefresh); err != nil {
		return nil, err
	}

	if err = b.WriteByte(subCommand); err != nil {
		return nil, err
	}

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

// createComShutdown generate COM_SHUTDOWN packet.
func (c *Conn) createComShutdown(level MyShutdownLevel) (*bytes.Buffer, error) {
	var err error

	payloadLength := 1 + // comShutdown
		1 // shutdown level length

	b := bytes.NewBuffer(make([]byte, packetHeaderSize+payloadLength))
	b.Next(4)

	if err = b.WriteByte(comShutdown); err != nil {
		return nil, err
	}

	if err = b.WriteByte(byte(level)); err != nil {
		return nil, err
	}

	return b, nil
}

// createComStatistics generates COM_STATISTICS packet.
func (c *Conn) createComStatistics() (*bytes.Buffer, error) {
	var err error

	payloadLength := 1 // comStatistics

	b := bytes.NewBuffer(make([]byte, packetHeaderSize+payloadLength))
	b.Next(4)

	if err = b.WriteByte(comStatistics); err != nil {
		return nil, err
	}

	return b, nil
}

// createComProcessInfo generates COM_PROCESS_INFO packet.
func (c *Conn) createComProcessInfo() (*bytes.Buffer, error) {
	var err error

	payloadLength := 1 // comProcessInfo

	b := bytes.NewBuffer(make([]byte, packetHeaderSize+payloadLength))
	b.Next(4)

	if err = b.WriteByte(comProcessInfo); err != nil {
		return nil, err
	}

	return b, nil
}

func (c *Conn) parseColumnDefinitionPacket(b *bytes.Buffer, isComFieldList bool) *columnDefinition {
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

func (c *Conn) parseResultSetRowPacket(b *bytes.Buffer, columnCount uint64) *row {
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

func (c *Conn) parseBinaryResultSetRowPacket(b *bytes.Buffer, columnCount uint64) *row {
	r := new(row)
	r.columns = make([]interface{}, columnCount)

	b.Next(1) // packet header [00]

	// nullBitmap := b.Next(int((columnCount + 9) / 8))

	for i := uint64(0); i < columnCount; i++ {
		// TODO: parse typed column data.
	}

	return nil
}

func (c *Conn) handleComQueryResponse(b *bytes.Buffer) {
}
