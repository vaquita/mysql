/*
  The MIT License (MIT)

  Copyright (c) 2015 Nirbhay Choubey

  Permission is hereby granted, free of charge, to any person obtaining a copy
  of this software and associated documentation files (the "Software"), to deal
  in the Software without restriction, including without limitation the rights
  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
  copies of the Software, and to permit persons to whom the Software is
  furnished to do so, subject to the following conditions:

  The above copyright notice and this permission notice shall be included in all
  copies or substantial portions of the Software.

  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
  SOFTWARE.
*/

package mysql

import (
	"database/sql/driver"
	"encoding/binary"
	"math"
	"math/big"
	"strconv"
	"time"
)

// createComStmtPrepare generates the COM_STMT_PREPARE packet.
func (c *Conn) createComStmtPrepare(query string) ([]byte, error) {
	var (
		b                  []byte
		off, payloadLength int
		err                error
	)

	payloadLength = 1 + // _COM_STMT_PREPARE
		len(query) // length of query

	if b, err = c.buff.Reset(4 + payloadLength); err != nil {
		return nil, err
	}

	off += 4 // placeholder for protocol packet header
	b[off] = _COM_STMT_PREPARE
	off++
	off += copy(b[off:], query)

	return b[0:off], nil
}

// createComStmtExecute generates the COM_STMT_EXECUTE packet.
func (c *Conn) createComStmtExecute(s *Stmt, args []driver.Value) ([]byte, error) {
	var (
		b, nullBitmap                                  []byte
		off, payloadLength, nullBitmapSize, paramCount int
		err                                            error
	)

	// TODO : assert(s.paramCount == len(args))
	paramCount = int(s.paramCount)

	// null bitmap, size = (paramCount + 7) / 8
	nullBitmapSize = int((paramCount + 7) / 8)

	payloadLength = int(comStmtExecutePayloadLength(s, args))

	if b, err = c.buff.Reset(4 + payloadLength); err != nil {
		return nil, err
	}

	off += 4 // placeholder for protocol packet header

	b[off] = _COM_STMT_EXECUTE
	off++

	binary.LittleEndian.PutUint32(b[off:off+4], s.id)
	off += 4

	b[off] = s.flags
	off++

	binary.LittleEndian.PutUint32(b[off:off+4], s.iterationCount)
	off += 4

	if paramCount > 0 {
		nullBitmap = b[off : off+nullBitmapSize]
		// clear the null bitmap
		zerofy(nullBitmap)
		off += nullBitmapSize

		b[off] = s.newParamsBoundFlag
		off++

		if s.newParamsBoundFlag == 1 {
			poff := off // offset to keep track of parameter types
			off += (2 * int(s.paramCount))

			for i := 0; i < int(s.paramCount); i++ {
				switch v := args[i].(type) {
				case int64:
					binary.LittleEndian.PutUint16(b[poff:poff+2], uint16(_TYPE_LONG_LONG))
					poff += 2
					off += writeUint64(b[off:], uint64(v))
				case float64:
					binary.LittleEndian.PutUint16(b[poff:poff+2],
						uint16(_TYPE_DOUBLE))
					poff += 2
					off += writeDouble(b[off:], v)
				case bool:
					binary.LittleEndian.PutUint16(b[poff:poff+2],
						uint16(_TYPE_TINY))
					poff += 2
					value := uint8(0)
					if v == true {
						value = 1
					}
					off += writeUint8(b[off:], value)
				case []byte:
					binary.LittleEndian.PutUint16(b[poff:poff+2],
						uint16(_TYPE_BLOB))
					poff += 2
					off += writeString(b[off:], string(v))
				case string:
					binary.LittleEndian.PutUint16(b[poff:poff+2],
						uint16(_TYPE_VARCHAR))
					poff += 2
					off += writeString(b[off:], v)
				case time.Time:
					binary.LittleEndian.PutUint16(b[poff:poff+2],
						uint16(_TYPE_TIMESTAMP))
					poff += 2
					off += writeDate(b[off:], v)
				case nil:
					binary.LittleEndian.PutUint16(b[poff:poff+2],
						uint16(_TYPE_NULL))
					poff += 2
					// set the corresponding null bit
					nullBitmap[int(i/8)] |= 1 << uint(i%8)
				default:
					// TODO: handle error
				}
			}
		}
	}

	return b[0:off], nil
}

// createComStmtClose generates the COM_STMT_CLOSE packet.
func (c *Conn) createComStmtClose(sid uint32) ([]byte, error) {
	var (
		b                  []byte
		off, payloadLength int
		err                error
	)

	payloadLength = 5 // _COM_STMT_CLOSE(1) + s.id(4)

	if b, err = c.buff.Reset(4 + payloadLength); err != nil {
		return nil, err
	}

	off += 4 // placeholder for protocol packet header

	b[off] = _COM_STMT_CLOSE
	off++

	binary.LittleEndian.PutUint32(b[off:off+4], sid)
	off += 4

	return b[0:off], nil
}

// createComStmtReset generates the COM_STMT_RESET packet.
func (c *Conn) createComStmtReset(s *Stmt) ([]byte, error) {
	var (
		b                  []byte
		off, payloadLength int
		err                error
	)

	payloadLength = 5 // _COM_STMT_RESET (1) + s.id (4)

	if b, err = c.buff.Reset(4 + payloadLength); err != nil {
		return nil, err
	}

	off += 4 // placeholder for protocol packet header

	b[off] = _COM_STMT_RESET
	off++

	binary.LittleEndian.PutUint32(b[off:off+4], s.id)
	off += 4

	return b[0:off], nil
}

// createComStmtSendLongData generates the COM_STMT_SEND_LONG_DATA packet.
func (c *Conn) createComStmtSendLongData(s *Stmt, paramId uint16, data []byte) ([]byte, error) {
	var (
		b                  []byte
		off, payloadLength int
		err                error
	)

	payloadLength = 7 + // _COM_STMT_SEND_LONG_DATA(1) + s.id(4) + paramId(2)
		len(data) // length of data

	if b, err = c.buff.Reset(4 + payloadLength); err != nil {
		return nil, err
	}

	off += 4 // placeholder for protocol packet header

	b[off] = _COM_STMT_SEND_LONG_DATA
	off++

	binary.LittleEndian.PutUint32(b[off:off+4], s.id)
	off += 4
	binary.LittleEndian.PutUint16(b[off:off+2], paramId)
	off += 2

	return b[0:off], nil
}

// handleStmtPrepare handles COM_STMT_PREPARE and related packets
func (c *Conn) handleStmtPrepare(query string) (*Stmt, error) {
	var (
		b   []byte
		err error
	)

	// reset the protocol packet sequence number
	c.resetSeqno()

	if b, err = c.createComStmtPrepare(query); err != nil {
		return nil, err
	}

	// write COM_STMT_PREPARE packet
	if err = c.writePacket(b); err != nil {
		return nil, err
	}

	// handle the response
	return c.handleComStmtPrepareResponse()
}

func (c *Conn) handleComStmtPrepareResponse() (*Stmt, error) {
	var (
		s    *Stmt
		b    []byte
		warn bool
		err  error
	)

	s = new(Stmt)
	s.c = c

	s.paramDefs = make([]*ColumnDefinition, 0)
	s.columnDefs = make([]*ColumnDefinition, 0)

	// read COM_STMT_PREPARE_OK packet.
	if b, err = c.readPacket(); err != nil {
		return nil, err
	}

	switch b[0] {
	case _PACKET_OK: // COM_STMT_PREPARE_OK packet
		warn = s.parseStmtPrepareOkPacket(b)
	case _PACKET_ERR:
		c.parseErrPacket(b)
		return nil, &c.e
	default:
		return nil, myError(ErrInvalidPacket)
	}

	more := s.paramCount > 0 // more packets ?

	// parameter definition block: read param definition packet(s)
	for more {
		if b, err = c.readPacket(); err != nil {
			return nil, err
		}

		switch b[0] {
		case _PACKET_EOF: // EOF packet, done!
			warn = c.parseEOFPacket(b)
			more = false
		default: // column definition packet
			s.paramDefs = append(s.paramDefs, parseColumnDefinitionPacket(b, false))
		}
	}

	more = s.columnCount > 0

	for more {
		if b, err = c.readPacket(); err != nil {
			return nil, err
		}

		switch b[0] {
		case _PACKET_EOF: // EOF packet, done!
			warn = c.parseEOFPacket(b)
			more = false
		default: // column definition packet
			s.columnDefs = append(s.columnDefs, parseColumnDefinitionPacket(b, false))
		}
	}

	if warn {
		return s, &c.e
	}

	return s, nil
}

// parseStmtPrepareOk parses COM_STMT_PREPARE_OK packet.
func (s *Stmt) parseStmtPrepareOkPacket(b []byte) bool {
	var off int

	off++ // [00] OK
	s.id = binary.LittleEndian.Uint32(b[off : off+4])
	off += 4
	s.columnCount = binary.LittleEndian.Uint16(b[off : off+2])
	off += 2
	s.paramCount = binary.LittleEndian.Uint16(b[off : off+2])
	off += 2
	off++ // reserved [00] filler
	s.warnings = binary.LittleEndian.Uint16(b[off : off+2])
	off += 2

	s.c.warnings = s.warnings
	return s.c.reportWarnings()
}

// handleExec handles COM_STMT_EXECUTE and related packets for Stmt's Exec()
func (s *Stmt) handleExec(args []driver.Value) (*Result, error) {
	var (
		b   []byte
		err error
	)

	// reset the protocol packet sequence number
	s.c.resetSeqno()

	// TODO: set me appropriately
	s.newParamsBoundFlag = 1

	if b, err = s.c.createComStmtExecute(s, args); err != nil {
		return nil, err
	}

	// send COM_STMT_EXECUTE to the server
	if err = s.c.writePacket(b); err != nil {
		return nil, err
	}

	return s.handleExecResponse()
}

// handleExecute handles COM_STMT_EXECUTE and related packets for Stmt's Query()
func (s *Stmt) handleQuery(args []driver.Value) (*Rows, error) {
	var (
		b   []byte
		err error
	)

	// reset the protocol packet sequence number
	s.c.resetSeqno()

	// TODO: set me appropriately
	s.newParamsBoundFlag = 1

	if b, err = s.c.createComStmtExecute(s, args); err != nil {
		return nil, err
	}

	// send COM_STMT_EXECUTE to the server
	if err := s.c.writePacket(b); err != nil {
		return nil, err
	}

	return s.handleQueryResponse()
}

// comStmtExecutePayloadLength returns the payload size of COM_STMT_EXECUTE
// packet.
func comStmtExecutePayloadLength(s *Stmt, args []driver.Value) (length uint64) {
	length = 1 + //_COM_STMT_PREPARE
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
		err  error
		b    []byte
		warn bool
	)

	c := s.c

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
		break

	case _PACKET_INFILE_REQ: // expected
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

	if warn {
		// command resulted in warning(s), return results and error
		return res, &c.e
	}

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
	case _PACKET_ERR: // expected
		// handle err packet
		c.parseErrPacket(b)
		return nil, &c.e

	case _PACKET_OK: // unexpected!
		// the command resulted in a Result (anti-pattern ?); but
		// since it succeeded we handle it and return nil.
		if c.parseOkPacket(b) {
			// the command resulted in warning(s)
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
	return c.handleBinaryResultSet(uint16(columnCount))
}

func (c *Conn) handleBinaryResultSet(columnCount uint16) (*Rows, error) {
	var (
		err        error
		b          []byte
		done, warn bool
	)

	rs := new(Rows)
	rs.columnDefs = make([]*ColumnDefinition, 0)
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
				c.handleBinaryResultSetRow(b, rs))
		}
	}

	if warn {
		// command resulted in warning(s), return results and error
		return rs, &c.e
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
		if isNull(nullBitmap, i, 2) == true {
			r.columns = append(r.columns, nil)
		} else {
			switch rs.columnDefs[i].ColumnType {
			// string
			case _TYPE_STRING, _TYPE_VARCHAR,
				_TYPE_VARSTRING, _TYPE_ENUM,
				_TYPE_SET, _TYPE_BLOB,
				_TYPE_TINY_BLOB, _TYPE_MEDIUM_BLOB,
				_TYPE_LONG_BLOB, _TYPE_GEOMETRY,
				_TYPE_BIT, _TYPE_DECIMAL,
				_TYPE_NEW_DECIMAL:
				v, n := parseString(b[off:])
				r.columns = append(r.columns, v)
				off += n

			// uint64
			case _TYPE_LONG_LONG:
				r.columns = append(r.columns, parseUint64(b[off:off+8]))
				off += 8

			// uint32
			case _TYPE_LONG, _TYPE_INT24:
				r.columns = append(r.columns, parseUint32(b[off:off+4]))
				off += 4

			// uint16
			case _TYPE_SHORT, _TYPE_YEAR:
				r.columns = append(r.columns, parseUint16(b[off:off+2]))
				off += 2

			// uint8
			case _TYPE_TINY:
				r.columns = append(r.columns, parseUint8(b[off:off+1]))
				off++

			// float64
			case _TYPE_DOUBLE:
				r.columns = append(r.columns, parseDouble(b[off:off+8]))
				off += 8

			// float32
			case _TYPE_FLOAT:
				r.columns = append(r.columns, parseFloat(b[off:off+4]))
				off += 4

			// time.Time
			case _TYPE_DATE, _TYPE_DATETIME,
				_TYPE_TIMESTAMP:
				v, n := parseDate(b[off:])
				r.columns = append(r.columns, v)
				off += n

			// time.Duration
			case _TYPE_TIME:
				v, n := parseTime(b[off:])
				r.columns = append(r.columns, v)
				off += n

			// TODO: map the following unhandled types accordingly
			case _TYPE_NEW_DATE, _TYPE_TIMESTAMP2,
				_TYPE_DATETIME2, _TYPE_TIME2,
				_TYPE_NULL:
				fallthrough
			default:
			}
		}
	}
	return r
}

// mysql data types (unexported)
const (
	_TYPE_DECIMAL = iota
	_TYPE_TINY
	_TYPE_SHORT
	_TYPE_LONG
	_TYPE_FLOAT
	_TYPE_DOUBLE
	_TYPE_NULL
	_TYPE_TIMESTAMP
	_TYPE_LONG_LONG
	_TYPE_INT24
	_TYPE_DATE
	_TYPE_TIME
	_TYPE_DATETIME
	_TYPE_YEAR
	_TYPE_NEW_DATE
	_TYPE_VARCHAR
	_TYPE_BIT
	_TYPE_TIMESTAMP2
	_TYPE_DATETIME2
	_TYPE_TIME2
	// ...
	_TYPE_NEW_DECIMAL = 246
	_TYPE_ENUM        = 247
	_TYPE_SET         = 248
	_TYPE_TINY_BLOB   = 249
	_TYPE_MEDIUM_BLOB = 250
	_TYPE_LONG_BLOB   = 251
	_TYPE_BLOB        = 252
	_TYPE_VARSTRING   = 253
	_TYPE_STRING      = 254
	_TYPE_GEOMETRY    = 255
)

// <!-- binary protocol value -->

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

func parseInt64(b []byte) int64 {
	return getInt64(b[:8])
}

func parseInt32(b []byte) int32 {
	return getInt32(b[:4])
}

func parseInt16(b []byte) int16 {
	return getInt16(b[:2])
}

func parseInt8(b []byte) int8 {
	return int8(b[0])
}

func parseDouble(b []byte) float64 {
	return math.Float64frombits(binary.LittleEndian.Uint64(b[:8]))
}

func parseFloat(b []byte) float32 {
	return math.Float32frombits(binary.LittleEndian.Uint32(b[:4]))
}

func parseNewDecimal(b []byte, size uint16) (float64, int) {
	var scale, precision int = int(size >> 8), int(size & 0xff)
	decimalSize := getDecimalBinarySize(precision, scale)

	positive := (b[0] & 0x80) == 0x80
	b[0] ^= 0x80

	if !positive {
		for i := 0; i < decimalSize; i++ {

			b[i] ^= 0xFF
		}
	}
	x := precision - scale
	ipDigits := x / _DIGITS_PER_INTEGER
	ipDigitsX := x - ipDigits*_DIGITS_PER_INTEGER
	ipSize := (ipDigits << 2) + _DIGITS_TO_BYTES[ipDigitsX]
	offset := _DIGITS_TO_BYTES[ipDigitsX]

	var value string

	if !positive {
		value += "-"
	}

	if offset > 0 {
		test := bigEndianInteger(b, 0, offset)
		value += strconv.FormatUint(uint64(test), 10)

	}

	for ; offset < ipSize; offset += 4 {
		value += strconv.FormatUint(uint64(bigEndianInteger(b, 0, offset)), 10)
	}
	shift := 0
	value += "."

	for ; shift+_DIGITS_PER_INTEGER <= scale; shift += _DIGITS_PER_INTEGER {
		value += strconv.FormatUint(uint64(bigEndianInteger(b, offset, 4)), 10)
		offset += 4
	}

	if shift < scale {
		value += strconv.FormatUint(uint64(bigEndianInteger(b, offset, _DIGITS_TO_BYTES[scale-shift])), 10)
	}

	rat, _ := new(big.Rat).SetString(value)
	result, _ := rat.Float64()
	return result, decimalSize
}

func getDecimalBinarySize(precision, scale int) int {
	x := precision - scale
	ipd := x / _DIGITS_PER_INTEGER
	fpd := scale / _DIGITS_PER_INTEGER
	return (ipd << 2) + _DIGITS_TO_BYTES[x-ipd*_DIGITS_PER_INTEGER] +
		(fpd << 2) + _DIGITS_TO_BYTES[scale-fpd*_DIGITS_PER_INTEGER]
}

func bigEndianInteger(bytes []byte, offset int, length int) int {
	result := 0
	for i := offset; i < (offset + length); i++ {
		b := bytes[i]
		if b >= 0 {
			result = (result << 8) | int(b)
		} else {
			result = (result << 8) | int(b>>256)
		}
	}
	return result
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
	var (
		b   []byte
		err error
	)

	// reset the protocol packet sequence number
	s.c.resetSeqno()

	if b, err = s.c.createComStmtClose(s.id); err != nil {
		return err
	}

	// write COM_STMT_CLOSE packet
	if err := s.c.writePacket(b); err != nil {
		return err
	}

	// note: expect no response from the server
	return nil
}
