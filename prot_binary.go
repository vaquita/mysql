package mysql

import (
	"database/sql/driver"
	"encoding/binary"
	"math"
	"time"
)

// createComStmtPrepare generates the COM_STMT_PREPARE packet.
func createComStmtPrepare(query string) []byte {
	var (
		off int
	)

	payloadLength := 1 + // comStmtPrepare
		len(query) // length of query

	b := make([]byte, 4+payloadLength)
	off += 4 // placeholder for protocol packet header

	b[off] = comStmtPrepare
	off++

	off += copy(b[off:], query)
	return b
}

// createComStmtExecute generates the COM_STMT_EXECUTE packet.
func createComStmtExecute(s *Stmt, args []driver.Value) []byte {
	var (
		nullBitmap     []byte
		nullBitmapSize int
		paramCount     int
		off            int
	)

	// TODO : assert(s.paramCount == len(args))
	paramCount = int(s.paramCount)

	// null bitmap, size = (paramCount + 7) / 8
	nullBitmapSize = int((paramCount + 7) / 8)

	b := make([]byte, 4+comStmtExecutePayloadLength(s, args))
	off += 4 // placeholder for protocol packet header

	b[off] = comStmtExecute
	off++

	binary.LittleEndian.PutUint32(b[off:off+4], s.id)
	off += 4

	b[off] = s.flags
	off++

	binary.LittleEndian.PutUint32(b[off:off+4], s.iterationCount)
	off += 4

	if paramCount > 0 {
		nullBitmap = b[off : off+nullBitmapSize]
		off += nullBitmapSize

		b[off] = s.newParamsBoundFlag
		off++

		if s.newParamsBoundFlag == 1 {
			poff := off // offset to keep track of parameter types
			off += (2 * int(s.paramCount))

			for i := 0; i < int(s.paramCount); i++ {
				switch v := args[i].(type) {
				case int64:
					binary.LittleEndian.PutUint16(b[poff:poff+2], uint16(mysqlTypeLongLong))
					poff += 2
					off += writeUint64(b[off:], uint64(v))
				case float64:
					binary.LittleEndian.PutUint16(b[poff:poff+2],
						uint16(mysqlTypeDouble))
					poff += 2
					off += writeDouble(b[off:], v)
				case bool:
					binary.LittleEndian.PutUint16(b[poff:poff+2],
						uint16(mysqlTypeTiny))
					poff += 2
					value := uint8(0)
					if v == true {
						value = 1
					}
					off += writeUint8(b[off:], value)
				case []byte:
					binary.LittleEndian.PutUint16(b[poff:poff+2],
						uint16(mysqlTypeBlob))
					poff += 2
					off += writeString(b[off:], string(v))
				case string:
					binary.LittleEndian.PutUint16(b[poff:poff+2],
						uint16(mysqlTypeVarchar))
					poff += 2
					off += writeString(b[off:], v)
				case time.Time:
					binary.LittleEndian.PutUint16(b[poff:poff+2],
						uint16(mysqlTypeTimestamp))
					poff += 2
					off += writeDate(b[off:], v)
				case nil:
					binary.LittleEndian.PutUint16(b[poff:poff+2],
						uint16(mysqlTypeNull))
					poff += 2
					// set the corresponding null bit
					nullBitmap[int(i/8)] |= 1 << uint(i%8)
				default:
					// TODO: handle error
				}
			}
		}
	}

	return b
}

// createComStmtClose generates the COM_STMT_CLOSE packet.
func createComStmtClose(sid uint32) []byte {
	var off int

	payloadLength := 5 // comStmtClose(1) + s.id(4)

	b := make([]byte, 4+payloadLength)
	off += 4 // placeholder for protocol packet header

	b[off] = comStmtClose
	off++

	binary.LittleEndian.PutUint32(b[off:off+4], sid)
	off += 4

	return b
}

// createComStmtReset generates the COM_STMT_RESET packet.
func createComStmtReset(s *Stmt) []byte {
	var off int

	payloadLength := 5 // comStmtReset (1) + s.id (4)

	b := make([]byte, 4+payloadLength)
	off += 4 // placeholder for protocol packet header

	b[off] = comStmtReset
	off++

	binary.LittleEndian.PutUint32(b[off:off+4], s.id)
	off += 4

	return b
}

// createComStmtSendLongData generates the COM_STMT_SEND_LONG_DATA packet.
func createComStmtSendLongData(s *Stmt, paramId uint16, data []byte) []byte {
	var off int

	payloadLength := 7 + // comStmtSendLongData(1) + s.id(4) + paramId(2)
		len(data) // length of data

	b := make([]byte, 4+payloadLength)
	off += 4 // placeholder for protocol packet header

	b[off] = comStmtSendLongData
	off++

	binary.LittleEndian.PutUint32(b[off:off+4], s.id)
	off += 4
	binary.LittleEndian.PutUint16(b[off:off+2], paramId)
	off += 2

	return b
}

// handleStmtPrepare handles COM_STMT_PREPARE and related packets
func (c *Conn) handleStmtPrepare(query string) (s *Stmt, err error) {
	// reset the protocol packet sequence number
	c.resetSeqno()

	// write COM_STMT_PREPARE packet
	if err = c.writePacket(createComStmtPrepare(query)); err != nil {
		return
	}

	// handle the response
	return c.handleComStmtPrepareResponse()
}

func (c *Conn) handleComStmtPrepareResponse() (s *Stmt, err error) {
	var b []byte

	s = new(Stmt)
	s.c = c

	s.paramDefs = make([]*columnDefinition, 0)
	s.columnDefs = make([]*columnDefinition, 0)

	// read COM_STMT_PREPARE_OK packet.
	if b, err = c.readPacket(); err != nil {
		return
	}

	switch b[0] {
	case okPacket: // COM_STMT_PREPARE_OK packet
		s.parseStmtPrepareOkPacket(b)
	case errPacket:
		c.parseErrPacket(b)
		err = &c.e
		return
	}

	more := s.paramCount > 0 // more packets ?

	// parameter definition block: read param definition packet(s)
	for more {
		if b, err = c.readPacket(); err != nil {
			return
		}
		switch b[0] {
		case eofPacket: // EOF packet, done!
			c.parseEOFPacket(b)
			more = false
		default: // column definition packet
			s.paramDefs = append(s.paramDefs, parseColumnDefinitionPacket(b, false))
		}
	}

	more = s.columnCount > 0

	for more {
		if b, err = c.readPacket(); err != nil {
			return
		}
		switch b[0] {
		case eofPacket: // EOF packet, done!
			c.parseEOFPacket(b)
			more = false
		default: // column definition packet
			s.columnDefs = append(s.columnDefs, parseColumnDefinitionPacket(b, false))
		}
	}
	return
}

// parseStmtPrepareOk parses COM_STMT_PREPARE_OK packet.
func (s *Stmt) parseStmtPrepareOkPacket(b []byte) {
	var off int

	off++ // [00] OK
	s.id = binary.LittleEndian.Uint32(b[off : off+4])
	off += 4
	s.columnCount = binary.LittleEndian.Uint16(b[off : off+2])
	off += 2
	s.paramCount = binary.LittleEndian.Uint16(b[off : off+2])
	off += 2
	off++ // reserved [00] filler
	s.warningCount = binary.LittleEndian.Uint16(b[off : off+2])
	off += 2
}

// handleExec handles COM_STMT_EXECUTE and related packets for Stmt's Exec()
func (s *Stmt) handleExec(args []driver.Value) (*Result, error) {
	var err error

	// reset the protocol packet sequence number
	s.c.resetSeqno()

	// TODO: set me appropriately
	s.newParamsBoundFlag = 1

	// send COM_STMT_EXECUTE to the server
	if err = s.c.writePacket(createComStmtExecute(s, args)); err != nil {
		return nil, err
	}

	return s.handleExecResponse()
}

// handleExecute handles COM_STMT_EXECUTE and related packets for Stmt's Query()
func (s *Stmt) handleQuery(args []driver.Value) (*Rows, error) {
	// reset the protocol packet sequence number
	s.c.resetSeqno()

	// TODO: set me appropriately
	s.newParamsBoundFlag = 1

	// send COM_STMT_EXECUTE to the server
	if err := s.c.writePacket(createComStmtExecute(s, args)); err != nil {
		return nil, err
	}

	return s.handleQueryResponse()
}

// comStmtExecutePayloadLength returns the payload size of COM_STMT_EXECUTE
// packet.
func comStmtExecutePayloadLength(s *Stmt, args []driver.Value) (length uint64) {
	length = 1 + //comStmtPrepare
		9 // id(4) + flags(1) + iterationCount(4)

	if s.paramCount > 0 {
		// null bitmap, size = (paramCount + 7) / 8
		length += uint64((s.paramCount + 7) / 8)
		length++ // newParamBoundFlag(1)

		if s.newParamsBoundFlag == 1 {
			length += uint64(s.paramCount * 2) // type of each paramater
			for i := 0; i < int(s.paramCount); i++ {
				switch v := args[i].(type) {
				case int64, float64:
					length += 8
				case bool:
					length++
				case []byte:
					length +=
						uint64(lenencIntSize(len(v)) + len(v))
				case string:
					length +=
						uint64(lenencIntSize(len(v)) + len(v))
				case time.Time:
					length += uint64(dateSize(v))
				case nil: // noop
				default: // TODO: handle error
				}
			}

		}
	}
	return
}

func (s *Stmt) handleExecResponse() (*Result, error) {
	var (
		err error
		b   []byte
	)

	c := s.c

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
		_, err = c.handleBinaryResultSet(uint16(columnCount)) // Rows ignored!
		return nil, err
	}

	res := new(Result)
	res.lastInsertId = int64(c.lastInsertId)
	res.rowsAffected = int64(c.affectedRows)
	return res, nil
}

func (s *Stmt) handleQueryResponse() (*Rows, error) {
	var (
		err error
		b   []byte
	)

	c := s.c

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
	return c.handleBinaryResultSet(uint16(columnCount))
}

func (c *Conn) handleBinaryResultSet(columnCount uint16) (*Rows, error) {
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
				c.handleBinaryResultSetRow(b, rs))
		}
	}
	return rs, nil
}

func (c *Conn) handleBinaryResultSetRow(b []byte, rs *Rows) *row {
	var (
		nullBitmapSize int
		off            int
	)

	columnCount := rs.columnCount
	r := new(row)
	r.columns = make([]interface{}, 0, columnCount)

	off++ // packet header [00]

	// null bitmap
	nullBitmapSize = int((columnCount + 9) / 8)
	nullBitmap := b[off : off+nullBitmapSize]
	off += nullBitmapSize

	for i := uint16(0); i < columnCount; i++ {
		if isNull(nullBitmap, i) == true {
			r.columns = append(r.columns, nil)
		} else {
			switch rs.columnDefs[i].columnType {
			// string
			case mysqlTypeString, mysqlTypeVarchar,
				mysqlTypeVarString, mysqlTypeEnum,
				mysqlTypeSet, mysqlTypeBlob,
				mysqlTypeTinyBlob, mysqlTypeMediumBlob,
				mysqlTypeLongBlob, mysqlTypeGeometry,
				mysqlTypeBit, mysqlTypeDecimal,
				mysqlTypeNewDecimal:
				v, n := parseString(b[off:])
				r.columns = append(r.columns, v)
				off += n

			// uint64
			case mysqlTypeLongLong:
				r.columns = append(r.columns, parseUint64(b[off:off+8]))
				off += 8

			// uint32
			case mysqlTypeLong, mysqlTypeInt24:
				r.columns = append(r.columns, parseUint32(b[off:off+4]))
				off += 4

			// uint16
			case mysqlTypeShort, mysqlTypeYear:
				r.columns = append(r.columns, parseUint16(b[off:off+2]))
				off += 2

			// uint8
			case mysqlTypeTiny:
				r.columns = append(r.columns, parseUint8(b[off:off+1]))
				off++

			// float64
			case mysqlTypeDouble:
				r.columns = append(r.columns, parseDouble(b[off:off+8]))
				off += 8

			// float32
			case mysqlTypeFloat:
				r.columns = append(r.columns, parseFloat(b[off:off+4]))
				off += 4

			// time.Time
			case mysqlTypeDate, mysqlTypeDateTime,
				mysqlTypeTimestamp:
				v, n := parseDate(b[off:])
				r.columns = append(r.columns, v)
				off += n

			// time.Duration
			case mysqlTypeTime:
				v, n := parseTime(b[off:])
				r.columns = append(r.columns, v)
				off += n

			// TODO: map the following unhandled types accordingly
			case mysqlTypeNewDate, mysqlTypeTimeStamp2,
				mysqlTypeDateTime2, mysqlTypeTime2,
				mysqlTypeNull:
				fallthrough
			default:
			}
		}
	}
	return r
}

// isNull returns whether the column at the given position is NULL; the first
// column's position is 0.
func isNull(bitmap []byte, pos uint16) bool {
	// for binary protocol, result set row offset = 2
	pos += 2

	if (bitmap[pos/8] & (1 << (pos % 8))) != 0 {
		return true // null
	}
	return false // not null
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
	mysqlTypeVarchar
	mysqlTypeBit
	mysqlTypeTimeStamp2
	mysqlTypeDateTime2
	mysqlTypeTime2
	// ...
	mysqlTypeNewDecimal = 246
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

func parseString(b []byte) (string, int) {
	v, n := getLenencString(b)
	return v.value, n
}

func parseUint64(b []byte) uint64 {
	return binary.LittleEndian.Uint64(b[:8])
}

func parseUint32(b []byte) uint32 {
	return binary.LittleEndian.Uint32(b[:4])
}

func parseUint16(b []byte) uint16 {
	return binary.LittleEndian.Uint16(b[:2])
}

func parseUint8(b []byte) uint8 {
	return uint8(b[0])
}

func parseDouble(b []byte) float64 {
	return math.Float64frombits(binary.LittleEndian.Uint64(b[:8]))
}

func parseFloat(b []byte) float32 {
	return math.Float32frombits(binary.LittleEndian.Uint32(b[:4]))
}

// TODO: fix location
func parseDate(b []byte) (time.Time, int) {
	var (
		year, day, hour, min, sec, msec int
		month                           time.Month
		loc                             *time.Location = time.UTC
		off                             int
	)

	len := b[off]
	off++

	if len >= 4 {
		year = int(binary.LittleEndian.Uint16(b[off : off+2]))
		off += 2
		month = time.Month(b[off])
		off++
		day = int(b[off])
		off++
	}

	if len >= 7 {
		hour = int(b[off])
		off++
		min = int(b[off])
		off++
		sec = int(b[off])
		off++
	}

	if len == 11 {
		msec = int(binary.LittleEndian.Uint32(b[off : off+4]))
		off += 4
	}

	return time.Date(year, month, day, hour, min, sec, msec*1000, loc), off
}

func parseTime(b []byte) (time.Duration, int) {
	var (
		duration time.Duration
		neg      int // multiplier
		off      int
	)

	len := b[off]
	off++

	if len >= 8 {
		if b[off] == 1 {
			neg = -1
		} else {
			neg = 1
		}
		off++

		duration += time.Duration(binary.LittleEndian.Uint32(b[off:off+4])) *
			24 * time.Hour
		off += 4
		duration += time.Duration(b[off]) * time.Hour
		off++
		duration += time.Duration(b[off]) * time.Minute
		off++
		duration += time.Duration(b[off]) * time.Second
		off++
	}

	if len == 12 {
		duration +=
			time.Duration(binary.LittleEndian.Uint32(b[off:off+4])) *
				time.Microsecond
	}

	return time.Duration(neg) * duration, off
}

func writeString(b []byte, v string) (n int) {
	return putLenencString(b, v)
}

func writeUint64(b []byte, v uint64) (n int) {
	binary.LittleEndian.PutUint64(b[:8], v)
	return 8
}

func writeUint32(b []byte, v uint32) (n int) {
	binary.LittleEndian.PutUint32(b[:4], v)
	return 4
}

func writeUint16(b []byte, v uint16) (n int) {
	binary.LittleEndian.PutUint16(b[:2], v)
	return 2
}

func writeUint8(b []byte, v uint8) (n int) {
	b[0] = uint8(v)
	return 1
}

func writeDouble(b []byte, v float64) (n int) {
	binary.LittleEndian.PutUint64(b[:8], math.Float64bits(v))
	return 8
}

func writeFloat(b []byte, v float32) (n int) {
	binary.LittleEndian.PutUint32(b[:4], math.Float32bits(v))
	return 4
}

// TODO: Handle 0 date
func writeDate(b []byte, v time.Time) int {
	var (
		length, month, day, hour, min, sec uint8
		year                               uint16
		msec                               uint32
		off                                int
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
			return 0
		} else {
			length = 4
		}
	} else if msec == 0 {
		length = 7
	} else {
		length = 11
	}

	b[off] = length
	off++

	if length >= 4 {
		binary.LittleEndian.PutUint16(b[off:off+2], year)
		off += 2
		b[off] = month
		off++
		b[off] = day
		off++
	}

	if length >= 7 {
		b[off] = hour
		off++
		b[off] = min
		off++
		b[off] = sec
		off++
	}

	if length == 11 {
		binary.LittleEndian.PutUint32(b[off:off+4], msec)
		off += 4
	}

	return off
}

// dateSize returns the size needed to store a given time.Time.
func dateSize(v time.Time) (length uint8) {
	var (
		month, day, hour, min, sec uint8
		year                       uint16
		msec                       uint32
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
			return 0
		} else {
			length = 4
		}
	} else if msec == 0 {
		length = 7
	} else {
		length = 11
	}
	length++ // 1 extra byte needed to store the length itself
	return
}

func writeTime(b []byte, v time.Duration) int {
	var (
		length, neg, hours, mins, secs uint8
		days, msecs                    uint32
		off                            int
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
		return 0
	}

	if msecs == 0 {
		length = 8
	} else {
		length = 12
	}

	b[off] = length
	off++
	b[off] = neg
	off++

	if length >= 8 {
		binary.LittleEndian.PutUint32(b[off:off+4], days)
		off += 4
		b[off] = hours
		off++
		b[off] = mins
		off++
		b[off] = secs
		off++
	}

	if length == 12 {
		binary.LittleEndian.PutUint32(b[off:off+4], msecs)
		off += 4
	}
	return off
}

// handleClose handles COM_STMT_CLOSE and related packets
func (s *Stmt) handleClose() error {
	// reset the protocol packet sequence number
	s.c.resetSeqno()

	// write COM_STMT_CLOSE packet
	if err := s.c.writePacket(createComStmtClose(s.id)); err != nil {
		return err
	}

	// note: expect no response from the server
	return nil
}
