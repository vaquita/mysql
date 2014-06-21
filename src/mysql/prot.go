package mysql

import (
	"bytes"
	"encoding/binary"
	"math"
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
	b.Next(4) // placeholder for protocol packet header

	// client capability flags
	binary.LittleEndian.PutUint32(b.Next(4), c.clientCapabilityFlags)
	// max packaet size
	binary.LittleEndian.PutUint32(b.Next(4), c.maxPacketSize)
	b.WriteByte(c.clientCharacterSet) // client character set
	b.Next(23)                        // reserved (all [0])

	putNullTerminatedString(b, c.p.username)

	if (c.serverCapabilityFlags & clientPluginAuthLenencClientData) != 0 {
		putLenencString(b, c.authResponseData)
	} else if (c.serverCapabilityFlags & clientSecureConnection) != 0 {
		b.WriteByte(byte(len(c.authResponseData)))
		b.WriteString(c.authResponseData)
	} else {
		putNullTerminatedString(b, c.authResponseData)
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
func (c *Conn) createComQuit() (*bytes.Buffer, error) {
	payloadLength := 1 // comQuit

	b := bytes.NewBuffer(make([]byte, packetHeaderSize+payloadLength))
	b.Next(4) // placeholder for protocol packet header

	b.WriteByte(comQuit)

	return b, nil
}

// createComInitDb generates the COM_INIT_DB packet.
func (c *Conn) createComInitDb(schema string) (*bytes.Buffer, error) {
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
func (c *Conn) createComQuery(query string) (*bytes.Buffer, error) {
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
func (c *Conn) createComFieldList(table, fieldWildcard string) (*bytes.Buffer, error) {
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
func (c *Conn) createComCreateDb(schema string) (*bytes.Buffer, error) {
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

// createComDropDb generate the COM_DROP_DB packet.
func (c *Conn) createComDropDb(schema string) (*bytes.Buffer, error) {
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
func (c *Conn) createComRefresh(subCommand uint8) (*bytes.Buffer, error) {
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

// createComShutdown generate COM_SHUTDOWN packet.
func (c *Conn) createComShutdown(level MyShutdownLevel) (*bytes.Buffer, error) {
	payloadLength := 1 + // comShutdown
		1 // shutdown level length

	b := bytes.NewBuffer(make([]byte, packetHeaderSize+payloadLength))
	b.Next(4) // placeholder for protocol packet header

	b.WriteByte(comShutdown)
	b.WriteByte(byte(level))

	return b, nil
}

// createComStatistics generates COM_STATISTICS packet.
func (c *Conn) createComStatistics() (*bytes.Buffer, error) {
	payloadLength := 1 // comStatistics

	b := bytes.NewBuffer(make([]byte, packetHeaderSize+payloadLength))
	b.Next(4) // placeholder for protocol packet header

	b.WriteByte(comStatistics)

	return b, nil
}

// createComProcessInfo generates COM_PROCESS_INFO packet.
func (c *Conn) createComProcessInfo() (*bytes.Buffer, error) {
	payloadLength := 1 // comProcessInfo

	b := bytes.NewBuffer(make([]byte, packetHeaderSize+payloadLength))
	b.Next(4) // placeholder for protocol packet header

	b.WriteByte(comProcessInfo)

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

func (c *Conn) handleComQueryResponse(b *bytes.Buffer) {
}

//<!-- prepared statements -->

// createComStmtPrepare generates the COM_STMT_PREPARE packet.
func (c *Conn) createComStmtPrepare(query string) (*bytes.Buffer, error) {
	var err error

	payloadLength := 1 + // comStmtPrepare
		len(query) // length of query

	b := bytes.NewBuffer(make([]byte, packetHeaderSize+payloadLength))
	b.Next(4) // placeholder for protocol packet header

	b.WriteByte(comStmtPrepare)
	if _, err = b.WriteString(query); err != nil {
		return nil, err
	}

	return b, nil
}

// createComStmtExecute generates the COM_STMT_EXECUTE packet.
func (c *Conn) createComStmtExecute(s *Stmt) (*bytes.Buffer, error) {
	// calculate the payload length
	payloadLength := 1 + //comStmtPrepare
		9 // id(4) + flags(1) + iterationCount(4)
	if s.paramCount > 0 {
		payloadLength += int((s.paramCount + 7) / 8)
		payloadLength++ // newParamBoundFlag(1)

		if s.newParamsBoundFlag == 1 {
			payloadLength += int(s.paramCount * 2) // type of each paramater
			payloadLength += s.paramValueLength
		}
	}

	b := bytes.NewBuffer(make([]byte, packetHeaderSize+payloadLength))
	b.Next(4) // placeholder for protocol packet header

	b.WriteByte(comStmtExecute)
	binary.LittleEndian.PutUint32(b.Next(4), s.id)
	b.WriteByte(s.flags)
	binary.LittleEndian.PutUint32(b.Next(4), s.iterationCount)

	if s.paramCount > 0 {
		b.Write(s.nullBitmap) // NULL-bitmap, size: (paramCount+7)/8
		b.WriteByte(byte(s.newParamsBoundFlag))
		if s.newParamsBoundFlag == 1 {
			// type of each parameter
			for i := 0; i < int(s.paramCount); i++ {
				binary.LittleEndian.PutUint16(b.Next(2), s.paramType[i])
			}

			// value of each parameter
			for i := 0; i < int(s.paramCount); i++ {
			}
		}
	}

	return b, nil
}

// createComStmtClose generates the COM_STMT_CLOSE packet.
func (c *Conn) createComStmtClose(s *Stmt) (*bytes.Buffer, error) {
	payloadLength := 5 // comStmtClose(1) + s.id(4)

	b := bytes.NewBuffer(make([]byte, packetHeaderSize+payloadLength))
	b.Next(4) // placeholder for protocol packet header

	b.WriteByte(comStmtClose)
	binary.LittleEndian.PutUint32(b.Next(4), s.id)

	return b, nil
}

// createComStmtReset generates the COM_STMT_RESET packet.
func (c *Conn) createComStmtReset(s *Stmt) (*bytes.Buffer, error) {
	payloadLength := 5 // comStmtReset (1) + s.id (4)

	b := bytes.NewBuffer(make([]byte, packetHeaderSize+payloadLength))
	b.Next(4) // placeholder for protocol packet header

	b.WriteByte(comStmtReset)

	binary.LittleEndian.PutUint32(b.Next(4), s.id)

	return b, nil
}

// createComStmtSendLongData generates the COM_STMT_SEND_LONG_DATA packet.
func (c *Conn) createComStmtSendLongData(s *Stmt, paramId uint16, data []byte) (*bytes.Buffer, error) {
	payloadLength := 7 + // comStmtSendLongData(1) + s.id(4) + paramId(2)
		len(data) // length of data

	b := bytes.NewBuffer(make([]byte, packetHeaderSize+payloadLength))
	b.Next(4) // placeholder for protocol packet header

	b.WriteByte(comStmtSendLongData)
	binary.LittleEndian.PutUint32(b.Next(4), s.id)
	binary.LittleEndian.PutUint16(b.Next(2), paramId)

	return b, nil
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

// mysql data types
const (
	mysqlTypeDecimal = iota
	mysqlTypeTiny
	mysqlTypeShort
	mysqlTypeLong
	mysqlTypeFloat
	mysqlTypeDouble
	mysqlTypeNull
	mysqlTypeTimestamp
	mysqlTypeLongLong
	mysqlTypeInt24
	mysqlTypeDate
	mysqlTypeTime
	mysqlTypeDateTime
	mysqlTypeYear
	mysqlTypeNewDate
	mysqlTypeVarChar
	mysqlTypeBit
	mysqlTypeTimeStamp2
	mysqlTypeDateTime2
	mysqlTypeTime2
	// ...
	mysqlTypeNewDecimap = 246
	mysqlTypeEnum       = 247
	mysqlTypeSet        = 248
	mysqlTypeTinyBlob   = 249
	mysqlTypeMediumBlob = 250
	mysqlTypeLongBlob   = 251
	mysqlTypeBlob       = 252
	mysqlTypeVarString  = 253
	mysqlTypeString     = 254
	mysqlTypeGeometry   = 255
)

// <!-- binary protocol value -->

/*
  MySQL - Go type mapping
  -----------------------
  MYSQL_TYPE_DECIMAL
  MYSQL_TYPE_TINY
  MYSQL_TYPE_SHORT
  MYSQL_TYPE_LONG
  MYSQL_TYPE_FLOAT
  MYSQL_TYPE_DOUBLE
  MYSQL_TYPE_NULL
  MYSQL_TYPE_TIMESTAMP
  MYSQL_TYPE_LONGLONG
  MYSQL_TYPE_INT24
  MYSQL_TYPE_DATE
  MYSQL_TYPE_TIME
  MYSQL_TYPE_DATETIME
  MYSQL_TYPE_YEAR
  MYSQL_TYPE_NEWDATE
  MYSQL_TYPE_VARCHAR
  MYSQL_TYPE_BIT
  MYSQL_TYPE_TIMESTAMP2
  MYSQL_TYPE_DATETIME2
  MYSQL_TYPE_TIME2
  MYSQL_TYPE_NEWDECIMAL
  MYSQL_TYPE_ENUM
  MYSQL_TYPE_SET
  MYSQL_TYPE_TINY_BLOB
  MYSQL_TYPE_MEDIUM_BLOB
  MYSQL_TYPE_LONG_BLOB
  MYSQL_TYPE_BLOB
  MYSQL_TYPE_VAR_STRING
  MYSQL_TYPE_STRING
  MYSQL_TYPE_GEOMETRY
*/

func parseString(b *bytes.Buffer) string {
	return getLenencString(b).value
}

func parseUint64(b *bytes.Buffer) uint64 {
	return binary.LittleEndian.Uint64(b.Next(8))
}

func parseUint32(b *bytes.Buffer) uint32 {
	return binary.LittleEndian.Uint32(b.Next(4))
}

func parseUint16(b *bytes.Buffer) uint16 {
	return binary.LittleEndian.Uint16(b.Next(2))
}

func parseUint8(b *bytes.Buffer) uint8 {
	return uint8(b.Next(1)[0])
}

func parseDouble(b *bytes.Buffer) float64 {
	return math.Float64frombits(binary.LittleEndian.Uint64(b.Next(8)))
}

func parseFloat(b *bytes.Buffer) float32 {
	return math.Float32frombits(binary.LittleEndian.Uint32(b.Next(4)))
}

// TODO: fix location
func parseDate(b *bytes.Buffer) time.Time {
	var (
		year, day, hour, min, sec, msec int
		month                           time.Month
		loc                             *time.Location = time.UTC
	)

	len := b.Next(1)[0]

	if len >= 4 {
		year = int(binary.LittleEndian.Uint16(b.Next(2)))
		month = time.Month(b.Next(1)[0])
		day = int(b.Next(1)[0])
	}

	if len >= 7 {
		hour = int(b.Next(1)[0])
		min = int(b.Next(1)[0])
		sec = int(b.Next(1)[0])
	}

	if len == 11 {
		msec = int(binary.LittleEndian.Uint32(b.Next(4)))
	}

	return time.Date(year, month, day, hour, min, sec, msec*1000, loc)
}

func parseTime(b *bytes.Buffer) time.Duration {
	var (
		duration time.Duration
		neg      int // multiplier
	)

	len := b.Next(1)[0]

	if len >= 8 {
		if b.Next(1)[0] == 1 {
			neg = -1
		} else {
			neg = 1
		}

		duration += time.Duration(binary.LittleEndian.Uint32(b.Next(4))) *
			24 * time.Hour
		duration += time.Duration(b.Next(1)[0]) * time.Hour
		duration += time.Duration(b.Next(1)[0]) * time.Minute
		duration += time.Duration(b.Next(1)[0]) * time.Second
	}

	if len == 12 {
		duration += time.Duration(binary.LittleEndian.Uint32(b.Next(4))) *
			time.Microsecond
	}

	return time.Duration(neg) * duration
}

func parseNull() {
}

func writeString(b *bytes.Buffer, v string) {
	putLenencString(b, v)
}

func writeUint64(b *bytes.Buffer, v uint64) {
	binary.LittleEndian.PutUint64(b.Next(8), v)
}

func writeUint32(b *bytes.Buffer, v uint32) {
	binary.LittleEndian.PutUint32(b.Next(4), v)
}

func writeUint16(b *bytes.Buffer, v uint16) {
	binary.LittleEndian.PutUint16(b.Next(2), v)
}

func writeUint8(b *bytes.Buffer, v uint8) {
	b.WriteByte(v)
}

func writeDouble(b *bytes.Buffer, v float64) {
	binary.LittleEndian.PutUint64(b.Next(8), math.Float64bits(v))
}

func writeFloat(b *bytes.Buffer, v float32) {
	binary.LittleEndian.PutUint32(b.Next(4), math.Float32bits(v))
}

// TODO: Handle 0 date
func writeDate(b *bytes.Buffer, v time.Time) {
	var (
		length, month, day, hour, min, sec uint8
		year                               uint16
		msec                               uint32
	)

	year = uint16(v.Year())
	month = uint8(v.Month())
	day = uint8(v.Day())
	hour = uint8(v.Hour())
	min = uint8(v.Minute())
	sec = uint8(v.Second())
	msec = uint32(v.Nanosecond() / 1000)

	if hour == 0 && min == 0 && sec == 0 && msec == 0 {
		if year == 0 && month == 0 && day == 0 {
			return
		} else {
			length = 4
		}
	} else if msec == 0 {
		length = 7
	} else {
		length = 11
	}

	b.WriteByte(length)

	if length >= 4 {
		binary.LittleEndian.PutUint16(b.Next(2), year)
		b.WriteByte(month)
		b.WriteByte(day)
	}

	if length >= 7 {
		b.WriteByte(hour)
		b.WriteByte(min)
		b.WriteByte(sec)
	}

	if length == 11 {
		binary.LittleEndian.PutUint32(b.Next(4), msec)
	}

	return
}

func writeTime(b *bytes.Buffer, v time.Duration) {
	var (
		length, neg, hours, mins, secs uint8
		days, msecs                    uint32
	)

	if v < 0 {
		neg = 1
	} // else neg = 0, positive

	days = uint32(v / (time.Hour * 24))
	v = v % (time.Hour * 24)

	hours = uint8(v / time.Hour)
	v %= time.Hour

	mins = uint8(v / time.Minute)
	v %= time.Minute

	secs = uint8(v / time.Second)
	v %= time.Second

	msecs = uint32(v / time.Microsecond)

	if days == 0 && hours == 0 && mins == 0 && secs == 0 && msecs == 0 {
		return
	}

	if msecs == 0 {
		length = 8
	} else {
		length = 12
	}

	b.WriteByte(length)
	b.WriteByte(neg)

	if length >= 8 {
		binary.LittleEndian.PutUint32(b.Next(4), days)
		b.WriteByte(hours)
		b.WriteByte(mins)
		b.WriteByte(secs)
	}

	if length == 12 {
		binary.LittleEndian.PutUint32(b.Next(4), msecs)
	}
	return
}

func writeNull() {
}
