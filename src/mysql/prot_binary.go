package mysql

import (
	"bytes"
	"encoding/binary"
	"math"
	"time"
)

//<!-- prepared statements -->

// createComStmtPrepare generates the COM_STMT_PREPARE packet.
func createComStmtPrepare(query string) (*bytes.Buffer, error) {
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
func createComStmtExecute(s *Stmt) (*bytes.Buffer, error) {
	var (
		b              *bytes.Buffer
		paramType      *bytes.Buffer
		nullBitmap     []byte
		paramCount     int
		nullBitmapSize int
		err            error
	)

	paramCount = int(s.paramCount)

	// null bitmap, size = (paramCount + 7) / 8
	nullBitmapSize = int((paramCount + 7) / 8)

	// calculate the payload length
	payloadLength := 1 + //comStmtPrepare
		9 // id(4) + flags(1) + iterationCount(4)
	if paramCount > 0 {
		payloadLength += nullBitmapSize
		payloadLength++ // newParamBoundFlag(1)

		if s.newParamsBoundFlag == 1 {
			payloadLength += paramCount * 2 // type of each paramater
			payloadLength += s.paramValueLength
		}
	}

	b = bytes.NewBuffer(make([]byte, packetHeaderSize+payloadLength))
	b.Next(4) // placeholder for protocol packet header

	b.WriteByte(comStmtExecute)
	binary.LittleEndian.PutUint32(b.Next(4), s.id)
	b.WriteByte(s.flags)
	binary.LittleEndian.PutUint32(b.Next(4), s.iterationCount)

	if paramCount > 0 {
		nullBitmap = b.Next(nullBitmapSize)

		b.WriteByte(byte(s.newParamsBoundFlag))

		if s.newParamsBoundFlag == 1 {
			// type of each parameter
			paramType = bytes.NewBuffer(b.Next(2 * paramCount))

			for i := 0; i < int(s.paramCount); i++ {
				switch v := s.paramValue[i].(type) {
				case int64:
					binary.LittleEndian.PutUint16(paramType.Next(2),
						uint16(mysqlTypeLongLong))
					writeUint64(b, uint64(v))
				case float64:
					binary.LittleEndian.PutUint16(paramType.Next(2),
						uint16(mysqlTypeDouble))
					writeDouble(b, v)
				case bool:
					binary.LittleEndian.PutUint16(paramType.Next(2),
						uint16(mysqlTypeTiny))
					value := uint8(0)
					if v == true {
						value = 1
					}
					writeUint8(b, value)
				case []byte:
					binary.LittleEndian.PutUint16(paramType.Next(2),
						uint16(mysqlTypeBlob))
					writeBlob(b, v)
				case string:
					binary.LittleEndian.PutUint16(paramType.Next(2),
						uint16(mysqlTypeVarchar))
					writeString(b, v)
				case time.Time:
					binary.LittleEndian.PutUint16(paramType.Next(2),
						uint16(mysqlTypeTimestamp))
					writeDate(b, v)
				case nil:

					binary.LittleEndian.PutUint16(paramType.Next(2),
						uint16(mysqlTypeNull))
					// set the corresponding null bit
					nullBitmap[int(i/8)] |= 1 << uint(i%8)
				default:
					// error
				}
			}
		}
	}

	return b, err
}

// createComStmtClose generates the COM_STMT_CLOSE packet.
func createComStmtClose(s *Stmt) (*bytes.Buffer, error) {
	payloadLength := 5 // comStmtClose(1) + s.id(4)

	b := bytes.NewBuffer(make([]byte, packetHeaderSize+payloadLength))
	b.Next(4) // placeholder for protocol packet header

	b.WriteByte(comStmtClose)
	binary.LittleEndian.PutUint32(b.Next(4), s.id)

	return b, nil
}

// createComStmtReset generates the COM_STMT_RESET packet.
func createComStmtReset(s *Stmt) (*bytes.Buffer, error) {
	payloadLength := 5 // comStmtReset (1) + s.id (4)

	b := bytes.NewBuffer(make([]byte, packetHeaderSize+payloadLength))
	b.Next(4) // placeholder for protocol packet header

	b.WriteByte(comStmtReset)

	binary.LittleEndian.PutUint32(b.Next(4), s.id)

	return b, nil
}

// createComStmtSendLongData generates the COM_STMT_SEND_LONG_DATA packet.
func createComStmtSendLongData(s *Stmt, paramId uint16, data []byte) (*bytes.Buffer, error) {
	payloadLength := 7 + // comStmtSendLongData(1) + s.id(4) + paramId(2)
		len(data) // length of data

	b := bytes.NewBuffer(make([]byte, packetHeaderSize+payloadLength))
	b.Next(4) // placeholder for protocol packet header

	b.WriteByte(comStmtSendLongData)
	binary.LittleEndian.PutUint32(b.Next(4), s.id)
	binary.LittleEndian.PutUint16(b.Next(2), paramId)

	return b, nil
}

// parseStmtPrepareOk parses COM_STMT_PREPARE_OK packet.
func (s *Stmt) parseStmtPrepareOkPacket(b *bytes.Buffer) {
	b.Next(1) // [00] OK
	s.id = binary.LittleEndian.Uint32(b.Next(4))
	s.columnCount = binary.LittleEndian.Uint16(b.Next(2))
	s.paramCount = binary.LittleEndian.Uint16(b.Next(2))
	b.Next(1) // reserved [00] filler
	s.warningCount = binary.LittleEndian.Uint16(b.Next(2))
}

func (c *Conn) handleBinaryResultSetRow(b *bytes.Buffer, rs *Rows) *row {
	columnCount := rs.columnCount
	r := new(row)
	r.columns = make([]interface{}, columnCount)

	b.Next(1) // packet header [00]

	// null bitmap
	nullBitmap := b.Next(int((columnCount + 9) / 8))

	for i := uint16(0); i < columnCount; i++ {
		if isNull(nullBitmap, i) == true {
			r.columns = append(r.columns, nil)
		} else {
			switch rs.columnDefs[i].columnType {
			// string
			case mysqlTypeString:
				fallthrough
			case mysqlTypeVarchar:
				fallthrough
			case mysqlTypeVarString:
				fallthrough
			case mysqlTypeEnum:
				fallthrough
			case mysqlTypeSet:
				fallthrough
			case mysqlTypeBlob:
				fallthrough
			case mysqlTypeTinyBlob:
				fallthrough
			case mysqlTypeMediumBlob:
				fallthrough
			case mysqlTypeLongBlob:
				fallthrough
			case mysqlTypeGeometry:
				fallthrough
			case mysqlTypeBit:
				fallthrough
			case mysqlTypeDecimal:
				fallthrough
			case mysqlTypeNewDecimal:
				r.columns = append(r.columns, parseString(b))

			// uint64
			case mysqlTypeLongLong:
				r.columns = append(r.columns, parseUint64(b))

			// uint32
			case mysqlTypeLong:
				fallthrough
			case mysqlTypeInt24:
				r.columns = append(r.columns, parseUint32(b))

			// uint16
			case mysqlTypeShort:
				fallthrough
			case mysqlTypeYear:
				r.columns = append(r.columns, parseUint16(b))

			// uint8
			case mysqlTypeTiny:
				r.columns = append(r.columns, parseUint8(b))

			// float64
			case mysqlTypeDouble:
				r.columns = append(r.columns, parseDouble(b))

			// float32
			case mysqlTypeFloat:
				r.columns = append(r.columns, parseFloat(b))

			// time.Time
			case mysqlTypeDate:
				fallthrough
			case mysqlTypeDateTime:
				fallthrough
			case mysqlTypeTimestamp:
				r.columns = append(r.columns, parseDate(b))

			// time.Duration
			case mysqlTypeTime:
				r.columns = append(r.columns, parseTime(b))

			// TODO: map the following unhandled types accordingly
			case mysqlTypeNewDate:
				fallthrough
			case mysqlTypeTimeStamp2:
				fallthrough
			case mysqlTypeDateTime2:
				fallthrough
			case mysqlTypeTime2:
				fallthrough
			case mysqlTypeNull:
				fallthrough
			default:
			}
		}
	}
	return r
}

// isNull returns whether the column at position identified by columnPosition
// is NULL. columnPosition is the column's position starting with 0.
func isNull(nullBitmap []byte, columnPosition uint16) bool {
	// for binary protocol result set row offset = 2
	columnPosition += 2

	if (nullBitmap[columnPosition/8] & (1 << (columnPosition % 8))) == 1 {
		return true // null
	}
	return false // not null
}

func (c *Conn) handleComStmtPrepareResponse() (*Stmt, error) {
	var (
		err error
		b   []byte
	)
	s := new(Stmt)
	s.paramDefs = make([]*columnDefinition, 0)
	s.columnDefs = make([]*columnDefinition, 0)

	// read COM_STMT_PREPARE_OK packet.
	if b, err = c.readPacket(); err != nil {
		return nil, err
	}

	switch b[0] {
	case okPacket: // COM_STMT_PREPARE_OK packet
		s.parseStmtPrepareOkPacket(bytes.NewBuffer(b))
	case errPacket:
		c.parseErrPacket(bytes.NewBuffer(b))
		return nil, &c.e
	}

	// parameter definition block: read param definition packet(s)
	for i := uint16(0); i < s.paramCount; i++ {
		if b, err = c.readPacket(); err != nil {
			return nil, err
		} else {
			s.paramDefs = append(s.paramDefs,
				parseColumnDefinitionPacket(bytes.NewBuffer(b), false))
		}
	}

	// read EOF packet
	if b, err = c.readPacket(); err != nil {
		return nil, err
	} else {
		c.parseEOFPacket(bytes.NewBuffer(b))
	}

	// column definition block: read column definition packet(s)
	for i := uint16(0); i < s.columnCount; i++ {
		if b, err = c.readPacket(); err != nil {
			return nil, err
		} else {
			s.columnDefs = append(s.columnDefs,
				parseColumnDefinitionPacket(bytes.NewBuffer(b), false))
		}
	}

	// read EOF packet
	if b, err = c.readPacket(); err != nil {
		return nil, err
	} else {
		c.parseEOFPacket(bytes.NewBuffer(b))
	}

	return s, nil
}

func (c *Conn) handleBinaryResultSet() (*Rows, error) {
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
				c.handleBinaryResultSetRow(bytes.NewBuffer(b), rs))
		}
	}
	return rs, nil
}

func (c *Conn) handleComStmtExecuteResponse() (*Rows, error) {
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
	default: // result set
		return c.handleBinaryResultSet()
	}

	// control shouldn't reach here
	return nil, nil
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

func writeBlob(b *bytes.Buffer, v []byte) {
	putLenencBlob(b, v)
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
