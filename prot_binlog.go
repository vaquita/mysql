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
	"encoding/binary"
	"io"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	_LOG_EVENT_BINLOG_IN_USE_F    = 0x1
	_LOG_EVENT_THREAD_SPECIFIC_F  = 0x4
	_LOG_EVENT_SUPPRESS_USE_F     = 0x8
	_LOG_EVENT_ARTIFICIAL_F       = 0x20
	_LOG_EVENT_RELAY_LOG_F        = 0x40
	_LOG_EVENT_SKIP_REPLICATION_F = 0x8000
)

const (
	_EVENT_TYPE_OFFSET = 4
	_FLAGS_OFFSET      = 17
)

type netReader struct {
	conn        *Conn
	slave       binlogSlave
	nonBlocking bool

	first  bool
	eof    bool
	closed bool

	e         error
	nextEvent []byte
}

// init
func (nr *netReader) init(p properties) error {
	var (
		err  error
		port uint64
	)

	// initialize slave structure
	v := strings.Split(p.address, ":")
	nr.slave.host = v[0]

	if port, err = strconv.ParseUint(v[1], 10, 16); err != nil {
		return myError(ErrInvalidDSN, err)
	} else {
		nr.slave.port = uint16(port)
	}

	nr.slave.id = p.binlogSlaveId
	nr.slave.username = p.username
	nr.slave.password = p.password
	nr.slave.replicationRank = 0
	nr.slave.masterId = 0

	nr.nonBlocking = p.binlogDumpNonBlock

	// establish a connection with the master server
	if nr.conn, err = open(p); err != nil {
		nr.closed = true
		return err
	}

	// notify master about checksum awareness
	if p.binlogVerifyChecksum {
		if err = notifyChecksumAwareness(nr.conn); err != nil {
			return err
		}
	}

	// send COM_REGISTER_SLAVE to (master) server
	if err = nr.registerSlave(); err != nil {
		return err
	}

	return nil
}

type binlogSlave struct {
	id              uint32
	host            string
	username        string
	password        string
	port            uint16
	replicationRank uint32 // ??
	masterId        uint32 // ??
}

type event struct {
	header eventHeader
	body   []byte
}

func (nr *netReader) begin(index binlogIndex) error {
	return nr.binlogDump(index)
}

func (nr *netReader) binlogDump(index binlogIndex) error {
	var (
		b   []byte
		err error
	)

	c := nr.conn

	// reset the protocol packet sequence number
	c.resetSeqno()

	if b, err = c.createComBinlogDump(nr.slave, index, nr.nonBlocking); err != nil {
		return err
	}

	// send COM_BINLOG_DUMP packet to (master) server
	if err = c.writePacket(b); err != nil {
		return err
	}

	if err = nr.readEvent(); err != nil {
		nr.eof = true
		return err
	}

	nr.first = true
	return nil
}

func (nr *netReader) close() error {
	if err := nr.conn.Close(); err != nil {
		return err
	}
	nr.closed = true
	return nil
}

func (nr *netReader) registerSlave() error {
	var (
		err error
		b   []byte
	)

	c := nr.conn

	// reset the protocol packet sequence number
	c.resetSeqno()

	if b, err = c.createComRegisterSlave(nr.slave); err != nil {
		return err
	}

	// send COM_REGISTER_SLAVE packet to (master) server
	if err = c.writePacket(b); err != nil {
		return err
	}

	if b, err = c.readPacket(); err != nil {
		return err
	}

	switch b[0] {
	case _PACKET_OK: //expected
		// parse OK packet
		if warn := c.parseOkPacket(b); warn {
			return &c.e
		}

	case _PACKET_ERR: //expected
		// parse err packet
		c.parseErrPacket(b)
		return &c.e

	default: // unexpected
		return myError(ErrInvalidPacket)
	}

	return nil
}

func (nr *netReader) next() bool {
	var err error

	// reset last error
	nr.e = nil

	if nr.closed || nr.eof {
		return false
	}

	if nr.first { // first event has already been read
		nr.first = false
	} else if err = nr.readEvent(); err != nil { // read the next event
		nr.eof = true
		if err != io.EOF {
			nr.e = err
		}
		// no more events to read
		return false
	}
	return true
}

func (nr *netReader) event() []byte {
	return nr.nextEvent
}

func (nr *netReader) error() error {
	return nr.e
}

func (c *Conn) createComRegisterSlave(s binlogSlave) ([]byte, error) {
	var (
		b                  []byte
		off, payloadLength int
		err                error
	)

	payloadLength = 18 + len(s.host) + len(s.username) + len(s.password)

	if b, err = c.buff.Reset(4 + payloadLength); err != nil {
		return nil, err
	}

	off += 4 // placeholder for protocol packet header

	b[off] = _COM_REGISTER_SLAVE
	off++

	binary.LittleEndian.PutUint32(b[off:off+4], s.id)
	off += 4

	b[off] = uint8(len(s.host))
	off++
	copy(b[off:], s.host)
	off += len(s.host)

	b[off] = uint8(len(s.username))
	off++
	copy(b[off:], s.username)
	off += len(s.username)

	b[off] = uint8(len(s.password))
	off++
	copy(b[off:], s.password)
	off += len(s.password)

	binary.LittleEndian.PutUint16(b[off:off+2], s.port)
	off += 2

	binary.LittleEndian.PutUint32(b[off:off+4], s.replicationRank)
	off += 4

	binary.LittleEndian.PutUint32(b[off:off+4], s.masterId)
	off += 4

	return b[0:off], nil
}

func (c *Conn) createComBinlogDump(slave binlogSlave, index binlogIndex,
	nonBlocking bool) ([]byte, error) {
	var (
		b                  []byte
		off, payloadLength int
		err                error
	)

	payloadLength = 11 + len(index.file)

	if b, err = c.buff.Reset(4 + payloadLength); err != nil {
		return nil, err
	}

	off += 4 // placeholder for protocol packet header

	b[off] = _COM_BINLOG_DUMP
	off++

	binary.LittleEndian.PutUint32(b[off:off+4], index.position)
	off += 4
	if nonBlocking {
		// flags (0x01 : BINLOG_DUMP_NON_BLOCK)
		binary.LittleEndian.PutUint16(b[off:off+2], uint16(0x01))
	} else {
		binary.LittleEndian.PutUint16(b[off:off+2], uint16(0))
	}

	off += 2
	binary.LittleEndian.PutUint32(b[off:off+4], slave.id)
	off += 4

	off += copy(b[off:], index.file)

	return b[0:off], nil
}

func parseEventHeader(b []byte) (eventHeader, int) {
	var (
		off    int
		header eventHeader
	)

	header.timestamp = binary.LittleEndian.Uint32(b[off : off+4])
	off += 4
	header.type_ = b[off]
	off++
	header.serverId = binary.LittleEndian.Uint32(b[off : off+4])
	off += 4
	header.size = binary.LittleEndian.Uint32(b[off : off+4])
	off += 4
	header.position = binary.LittleEndian.Uint32(b[off : off+4])
	off += 4
	header.flags = binary.LittleEndian.Uint16(b[off : off+2])
	off += 2

	return header, off
}

func (nr *netReader) readEvent() error {
	var (
		err error
		b   []byte
	)

	c := nr.conn

	if b, err = c.readPacket(); err != nil {
		return err
	}

	switch b[0] {
	case _PACKET_OK: // expected
		// move past [00]
		nr.nextEvent = b[1:]

	case _PACKET_ERR: //expected
		// handle err packet
		c.parseErrPacket(b)

		return &c.e

	case _PACKET_EOF: // expected
		if warn := c.parseEOFPacket(b); warn {
			// save warning (if any)
			nr.e = &c.e
		}
		return io.EOF

	default: //unexpected
		return myError(ErrInvalidPacket)
	}

	return nil
}

func (b *Binlog) parseStartEventV3(buf []byte, ev *StartEventV3) (err error) {
	var off int

	ev.binlogVersion = binary.LittleEndian.Uint16(buf)
	off += 2

	ev.serverVersion = string(buf[off : off+50])
	off += 50

	ev.creationTime = time.Unix(int64(binary.LittleEndian.Uint32(buf[off:])), 0)

	return
}

func (b *Binlog) parseQueryEvent(buf []byte, ev *QueryEvent) (err error) {
	var (
		off          int
		schemaLength int
		varLength    int
	)

	ev.slaveProxyId = binary.LittleEndian.Uint32(buf)
	off += 4

	ev.executionTime = time.Unix(int64(binary.LittleEndian.Uint32(buf[off:])), 0)
	off += 4

	// move past schema length
	schemaLength = int(buf[off])
	off++

	ev.errorCode = binary.LittleEndian.Uint16(buf[off:])
	off += 2

	if b.desc.binlogVersion >= 4 {
		varLength = int(binary.LittleEndian.Uint16(buf[off:]))
		off += 2
	}

	ev.statusVars = string(buf[off : off+varLength])
	off += varLength

	ev.schema = string(buf[off : off+schemaLength])
	off += schemaLength
	off++

	ev.query = string(buf[off:])
	return nil
}

func (b *Binlog) parseRotateEvent(buf []byte, ev *RotateEvent) (err error) {
	var off int

	ev.position = binary.LittleEndian.Uint64(buf)
	off += 8

	ev.file = string(buf[off:])

	return
}

func (b *Binlog) parseIntvarEvent(buf []byte, ev *IntvarEvent) (err error) {
	var off int
	ev.type_ = uint8(buf[0])
	off++

	ev.value = binary.LittleEndian.Uint64(buf[off:])

	return
}

// parseLoadEvent parses LOAD_EVENT as well as NEW_LOAD_EVENT
func (b *Binlog) parseLoadEvent(buf []byte, ev *LoadEvent) (err error) {
	var off int

	ev.slaveProxyId = binary.LittleEndian.Uint32(buf[off:])
	off += 4

	ev.executionTime = time.Unix(int64(binary.LittleEndian.Uint32(buf[off:])), 0)
	off += 4

	ev.skipLines = binary.LittleEndian.Uint32(buf[off:])
	off += 4

	// table name length
	off++

	// schema name length
	off++

	ev.fieldCount = binary.LittleEndian.Uint32(buf[off:])
	off += 4

	if ev.header.type_ == LOAD_EVENT {
		ev.fieldTerminator = string(buf[off])
		off++

		ev.enclosedBy = string(buf[off])
		off++

		ev.lineTerminator = string(buf[off])
		off++

		ev.lineStart = string(buf[off])
		off++

		ev.escapedBy = string(buf[off])
		off++

		ev.optFlags = make([]byte, 1)
		ev.optFlags[0] = buf[off]
		off++

		ev.emptyFlags = uint8(buf[off])
		off++
	} else { // NEW_LOAD_EVENT
		var length int

		length = int(buf[off])
		off++
		ev.fieldTerminator = string(buf[off : off+length])
		off += length

		length = int(buf[off])
		off++
		ev.enclosedBy = string(buf[off : off+length])
		off += length

		length = int(buf[off])
		off++
		ev.lineTerminator = string(buf[off : off+length])
		off += length

		length = int(buf[off])
		off++
		ev.lineStart = string(buf[off : off+length])
		off += length

		length = int(buf[off])
		off++
		ev.escapedBy = string(buf[off : off+length])
		off += length

		length = int(buf[off])
		off++
		ev.optFlags = make([]byte, length)
		copy(ev.optFlags, buf[off:off+length])
		off += length
	}

	/*
	   we do not really need individual field name lengths as
	   field names are null-terminated.
	*/
	off += int(ev.fieldCount)

	ev.fields = make([]string, ev.fieldCount)

	var i, n int
	for i = 0; i < int(ev.fieldCount); i++ {
		ev.fields[i], n = getNullTerminatedString(buf[off:])
		off += n
	}

	ev.table, n = getNullTerminatedString(buf[off:])
	off += n

	ev.schema, n = getNullTerminatedString(buf[off:])
	off += n

	ev.file, n = getNullTerminatedString(buf[off:])
	off += n

	return
}

func (b *Binlog) parseSlaveEvent(buf []byte, ev *SlaveEvent) (err error) {

	var off int
	ev.masterPosition = binary.LittleEndian.Uint64(buf[off:])
	off += 8

	ev.masterPort = binary.LittleEndian.Uint16(buf[off:])
	off += 2

	var n int
	ev.masterHost, n = getNullTerminatedString(buf[off:])
	off += n

	ev.masterLog, n = getNullTerminatedString(buf[off:])
	off += n

	return
}

func (b *Binlog) parseCreateFileEvent(buf []byte, ev *CreateFileEvent) (err error) {
	var off int

	ev.fileId = binary.LittleEndian.Uint32(buf[off:])
	off += 4

	ev.data = buf[off:]
	return
}

func (b *Binlog) parseAppendBlockEvent(buf []byte, ev *AppendBlockEvent) (err error) {
	var off int

	ev.fieldId = binary.LittleEndian.Uint32(buf[off:])
	off += 4

	ev.data = buf[off:]
	return
}

func (b *Binlog) parseExecLoadEvent(buf []byte, ev *ExecLoadEvent) (err error) {
	var off int
	ev.fileId = binary.LittleEndian.Uint32(buf[off:])
	return
}

func (b *Binlog) parseDeleteFileEvent(buf []byte, ev *DeleteFileEvent) (err error) {
	ev.fileId = binary.LittleEndian.Uint32(buf[0:])
	return
}

func (b *Binlog) parseRandEvent(buf []byte, ev *RandEvent) (err error) {
	var off int

	ev.seed1 = binary.LittleEndian.Uint64(buf[off:])
	off += 8

	ev.seed2 = binary.LittleEndian.Uint64(buf[off:])

	return
}

func (b *Binlog) parseUserVarEvent(buf []byte, ev *UserVarEvent) (err error) {
	var off int

	// name length
	length := int(binary.LittleEndian.Uint32(buf[off:]))
	off += 4
	ev.name = string(buf[off : off+length])
	off += length

	if buf[off] != 0 {
		ev.null = true
	}
	off++

	if !ev.null {
		ev.type_ = uint8(buf[off])
		off++

		ev.charset = binary.LittleEndian.Uint32(buf[off:])
		off += 4

		var valueLen int
		valueLen = int(binary.LittleEndian.Uint32(buf[off:]))
		off += 4

		ev.value = make([]byte, valueLen)
		copy(ev.value, buf[off:off+valueLen])
		off += valueLen
	}

	// more data?
	if len(buf[off:]) > 0 {
		ev.flags = uint8(buf[off])
	}

	return
}

func (b *Binlog) parseFormatDescriptionEvent(buf []byte, ev *FormatDescriptionEvent) (err error) {
	var off int

	ev.binlogVersion = binary.LittleEndian.Uint16(buf)
	off += 2

	ev.serverVersion = string(buf[off : off+50])
	off += 50

	ev.creationTime = time.Unix(int64(binary.LittleEndian.Uint32(buf[off:])), 0)
	off += 4

	ev.commonHeaderLength = uint8(buf[off])
	off++

	// TODO: check server version and/or binlog version to see if it
	// supports event checksum. For now consider and store rest of
	// unread buffer to postHeaderLength.
	ev.postHeaderLength = buf[off:]

	// Checksum algorithm descriptor (1 byte), its placed right before the
	// checksum value (4 bytes), excluded by the caller
	ev.checksumAlg = uint8(ev.postHeaderLength[len(ev.postHeaderLength)-1])
	return
}

func (b *Binlog) parseRowsQueryLogEvent(buf []byte, ev *RowsQueryLogEvent) (err error) {
	length := buf[0]
	ev.query = string(buf[1:length])
	return
}

func (b *Binlog) parseXidEvent(buf []byte, ev *XidEvent) (err error) {
	ev.xid = binary.LittleEndian.Uint64(buf[0:])
	return
}

func (b *Binlog) parseBeginLoadQueryEvent(buf []byte, ev *BeginLoadQueryEvent) (err error) {
	var off int

	ev.fileId = binary.LittleEndian.Uint32(buf[off:])
	off += 4

	ev.data = buf[off:]
	return
}

func (b *Binlog) parseExecuteLoadQueryEvent(buf []byte, ev *ExecuteLoadQueryEvent) (err error) {
	var off int

	ev.slaveProxyId = binary.LittleEndian.Uint32(buf[off:])
	off += 4

	ev.executionTime = time.Unix(int64(binary.LittleEndian.Uint32(buf[off:])), 0)
	off += 4

	ev.schemaLength = buf[off]
	off++

	ev.errorCode = binary.LittleEndian.Uint16(buf[off:])
	off += 2

	ev.statusVarsLength = binary.LittleEndian.Uint16(buf[off:])
	off += 4

	ev.fileId = binary.LittleEndian.Uint32(buf[off:])
	off += 4

	ev.startPosition = binary.LittleEndian.Uint32(buf[off:])
	off += 4

	ev.endPosition = binary.LittleEndian.Uint32(buf[off:])
	off += 4

	ev.dupHandlingFlags = uint8(buf[off])

	return
}

func (b *Binlog) parseTableMapEvent(buf []byte, ev *TableMapEvent) (err error) {
	var (
		off    int
		length int
		i      uint64
	)

	if b.desc.postHeaderLength[ev.header.type_-1] == 6 {
		ev.tableId = uint64(binary.LittleEndian.Uint32(buf[off:]))
		off += 4
	} else {
		ev.tableId = getUint48(buf)
		off += 6
	}

	ev.flags = binary.LittleEndian.Uint16(buf[off:])
	off += 2

	length = int(buf[off])
	off++
	ev.schema = string(buf[off : off+length])
	off += length
	off++

	length = int(buf[off])
	off++
	ev.table = string(buf[off : off+length])
	off += length
	off++

	ev.columnCount, length = getLenencInt(buf[off:])
	off += length

	ev.columns = make([]EventColumn, ev.columnCount)

	// field type
	for i = 0; i < ev.columnCount; i++ {
		ev.columns[i].type_ = uint8(buf[off])
		off++
	}

	// field meta data
	var meta uint16

	_, length = getLenencInt(buf[off:])
	off += length

	// read meta data and store them into the respective columns
	// TODO: verify that buffer consumed is equal to the meta data size
	for i = 0; i < ev.columnCount; i++ {
		switch getMetaDataSize(ev.columns[i].type_) {
		case 2:
			meta = binary.LittleEndian.Uint16(buf[off:])
			off += 2
		case 1:
			meta = uint16(buf[off])
			off++
		default:
			meta = 0
		}
		ev.columns[i].meta = meta
	}

	// null bitmap
	nullBitmapSize := int((ev.columnCount + 7) / 8)
	nullBitmap := buf[off : off+nullBitmapSize]
	for i = 0; i < ev.columnCount; i++ {
		if isNull(nullBitmap, uint16(i), 0) {
			ev.columns[i].nullable = true
		}
	}

	return
}

func getMetaDataSize(type_ uint8) uint8 {
	switch type_ {
	case _TYPE_TINY_BLOB, _TYPE_BLOB, _TYPE_MEDIUM_BLOB, _TYPE_LONG_BLOB,
		_TYPE_DOUBLE, _TYPE_FLOAT, _TYPE_GEOMETRY, _TYPE_TIME2,
		_TYPE_DATETIME2, _TYPE_TIMESTAMP2:
		return 1

	case _TYPE_SET, _TYPE_ENUM, _TYPE_STRING, _TYPE_BIT, _TYPE_VARCHAR, _TYPE_NEW_DECIMAL:
		return 2

	default:
	}
	return 0
}

func (b *Binlog) parseIncidentEvent(buf []byte, ev *IncidentEvent) (err error) {
	var (
		off    int
		length int
	)

	ev.type_ = binary.LittleEndian.Uint16(buf[off:])
	off += 2

	length = int(buf[off])
	off++

	ev.message = string(buf[off : off+length])
	return
}

// Note: There was no after-image in v0.
func (b *Binlog) parseRowsEvent(buf []byte, ev *RowsEvent) (err error) {
	var (
		off    int
		length int
	)

	if b.desc.postHeaderLength[ev.header.type_-1] == 6 {
		ev.tableId = uint64(binary.LittleEndian.Uint32(buf[off:]))
		off += 4
	} else {
		ev.tableId = getUint48(buf)
		off += 6
	}

	ev.flags = binary.LittleEndian.Uint16(buf[off:])
	off += 2

	if b.desc.postHeaderLength[ev.header.type_-1] == 10 {
		length = int(binary.LittleEndian.Uint16(buf[off:])) - 2
		off += 2
		ev.extraData = buf[off : off+length]
		off += length
	}

	ev.columnCount, length = getLenencInt(buf[off:])
	off += length

	length = int((ev.columnCount + 7) / 8)
	ev.columnsPresentBitmap1 = buf[off : off+length]
	off += length
	if (ev.header.type_ == UPDATE_ROWS_EVENT_V1) ||
		(ev.header.type_ == UPDATE_ROWS_EVENT) {
		ev.columnsPresentBitmap2 = buf[off : off+length]
		off += length
	}

	ev.rows1.rows = make([]EventRow, 0)
	if (ev.header.type_ == UPDATE_ROWS_EVENT_V1) ||
		(ev.header.type_ == UPDATE_ROWS_EVENT) {
		ev.rows2.rows = make([]EventRow, 0)
	}

	var (
		n int
		r EventRow
	)

	for off < len(buf) {
		r, n = b.parseEventRow(buf[off:], ev.columnCount,
			ev.columnsPresentBitmap1)
		ev.rows1.rows = append(ev.rows1.rows, r)
		off += n
		if (ev.header.type_ == UPDATE_ROWS_EVENT_V1) ||
			(ev.header.type_ == UPDATE_ROWS_EVENT) {
			r, n = b.parseEventRow(buf[off:], ev.columnCount,
				ev.columnsPresentBitmap2)
			ev.rows2.rows = append(ev.rows2.rows, r)
			off += n
		}
	}

	return
}

func (b *Binlog) parseEventRow(buf []byte, columnCount uint64,
	columnsPresentBitmap []byte) (EventRow, int) {
	var (
		off int
		r   EventRow
	)

	r.columns = make([]interface{}, 0, columnCount)

	nullBitmapSize := int((setBitCount(columnsPresentBitmap) + 7) / 8)
	nullBitmap := buf[off : off+nullBitmapSize]
	off += nullBitmapSize

	for i := uint64(0); i < columnCount; i++ {
		if isNull(nullBitmap, uint16(i), 0) == true {
			r.columns = append(r.columns, nil)
		} else {
			switch b.tableMap.columns[i].type_ {
			// string
			case _TYPE_VARCHAR, _TYPE_VARSTRING:
				v, n := parseString2(buf[off:], b.tableMap.columns[i].meta)
				r.columns = append(r.columns, v)
				off += n

			case _TYPE_STRING, _TYPE_ENUM,
				_TYPE_SET, _TYPE_BLOB,
				_TYPE_TINY_BLOB, _TYPE_MEDIUM_BLOB,
				_TYPE_LONG_BLOB, _TYPE_GEOMETRY,
				_TYPE_BIT, _TYPE_DECIMAL:
				v, n := parseString(buf[off:])
				r.columns = append(r.columns, v)
				off += n
			case _TYPE_NEW_DECIMAL:
				v, n := parseNewDecimal(buf[off:], b.tableMap.columns[i].meta)
				r.columns = append(r.columns, v)
				off += n
			// int64
			case _TYPE_LONG_LONG:
				r.columns = append(r.columns, parseInt64(buf[off:off+8]))
				off += 8

			// int32
			case _TYPE_LONG, _TYPE_INT24:
				r.columns = append(r.columns, parseInt32(buf[off:off+4]))
				off += 4

			// int16
			case _TYPE_SHORT, _TYPE_YEAR:
				r.columns = append(r.columns, parseInt16(buf[off:off+2]))
				off += 2

			// int8
			case _TYPE_TINY:
				r.columns = append(r.columns, parseInt8(buf[off:off+1]))
				off++

			// float64
			case _TYPE_DOUBLE:
				r.columns = append(r.columns, parseDouble(buf[off:off+8]))
				off += 8

			// float32
			case _TYPE_FLOAT:
				r.columns = append(r.columns, parseFloat(buf[off:off+4]))
				off += 4

			// time.Time
			case _TYPE_DATE, _TYPE_DATETIME,
				_TYPE_TIMESTAMP:
				v, n := parseDate(buf[off:])
				r.columns = append(r.columns, v)
				off += n

			// time.Duration
			case _TYPE_TIME:
				v, n := parseTime(buf[off:])
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
	return r, off
}

func (b *Binlog) parseGtidLogEvent(buf []byte, ev *GtidLogEvent) {
	var off int
	ev.gtid.commitFlag = (buf[off] != 0)
	off++
	copy(ev.gtid.sourceId.data[0:], buf[off:off+16])
	off += 16
	ev.gtid.groupNumber = getInt64(buf[off:])
	return
}

func (b *Binlog) parsePreviousGtidsLogEvent(buf []byte, ev *PreviousGtidsLogEvent) {
	ev.data = make([]byte, len(buf))
	copy(ev.data, buf[:])
	return
}

func (b *Binlog) parseAnnotateRowsEvent(buf []byte, ev *AnnotateRowsEvent) {
	ev.query = string(buf)
	return
}

func (b *Binlog) parseBinlogCheckpointEvent(buf []byte, ev *BinlogCheckpointEvent) {
	var off int
	ev.fileLength = binary.LittleEndian.Uint32(buf[off:])
	off += 4
	ev.file = string(buf[off:])
	return
}

func (b *Binlog) parseGtidEvent(buf []byte, ev *GtidEvent) {
	var off int

	ev.gtid.seqno = binary.LittleEndian.Uint64(buf[off:])
	off += 8

	ev.gtid.domainId = binary.LittleEndian.Uint32(buf[off:])
	off += 4

	ev.flags = uint8(buf[off])
	off++

	if (ev.flags & FL_GROUP_COMMIT_ID) != 0 {
		ev.commitId = binary.LittleEndian.Uint64(buf[off:])
	}

	ev.gtid.serverId = ev.header.serverId

	return
}

func (b *Binlog) parseGtidListEvent(buf []byte, ev *GtidListEvent) {
	var off int
	val := binary.LittleEndian.Uint32(buf[off:])
	off += 4

	ev.count = val & ((1 << 28) - 1)
	ev.flags = uint8(val & (0xF << 28))
	off++

	ev.list = make([]MariadbGtid, ev.count)

	for i := uint32(0); i < ev.count; i++ {
		ev.list[i].domainId = binary.LittleEndian.Uint32(buf[off:])
		ev.list[i].serverId = binary.LittleEndian.Uint32(buf[off+4:])
		ev.list[i].seqno = binary.LittleEndian.Uint64(buf[off+8:])
		off += 16
	}

	return
}

type fileReader struct {
	name      string
	file      *os.File
	closed    bool
	first     bool
	eof       bool
	e         error
	nextEvent []byte
}

func (fr *fileReader) begin(index binlogIndex) error {
	var err error

	if index.file != "" && index.file != fr.name {
		fr.name = index.file

		// close the previously opened file
		if !fr.closed {
			if err = fr.close(); err != nil {
				return myError(ErrFile, err)
			}
		}
		// open the new file
		if fr.file, err = os.Open(fr.name); err != nil {
			return myError(ErrFile, err)
		}
		fr.closed = false
	}

	if index.position > 0 {
		if _, err = fr.file.Seek(int64(index.position), 0); err != nil {
			// seek operation failed
			return myError(ErrFile, err)
		}
	}

	// read and verify magic number
	magic := make([]byte, 4)
	if _, err = fr.file.Read(magic); err != nil {
		return myError(ErrFile, err)

	} else {
		// TODO: verify magic number [0xfe, 'b', 'i', 'n']
	}
	if err = fr.readEvent(); err != nil {
		return err
	}

	fr.first = true
	return nil
}

func (fr *fileReader) close() error {
	var err error

	if !fr.closed {
		if err = fr.file.Close(); err != nil {
			// file close operation failed
			return myError(ErrFile, err)
		}
	}
	fr.closed = true
	return nil
}

func (fr *fileReader) next() bool {
	var err error

	// reset last error
	fr.e = nil

	if fr.eof {
		return false
	}

	if fr.first { // first event has already been read
		fr.first = false
	} else if err = fr.readEvent(); err != nil { // read the next event
		fr.eof = true
		if err != io.EOF {
			fr.e = err
		}
		return false
	}
	return true
}

func (fr *fileReader) event() []byte {
	return fr.nextEvent
}

func (fr *fileReader) init(p properties) error {
	var err error
	fr.name = p.file

	if fr.file, err = os.Open(fr.name); err != nil {
		fr.closed = true
		return myError(ErrFile, err)
	}
	fr.closed = false
	return nil
}

func (fr *fileReader) readEvent() error {
	var (
		err                          error
		headerBuf, bodyBuf, eventBuf []byte
		header                       eventHeader
	)

	// read the binlog header
	headerBuf = make([]byte, 19)
	if _, err = fr.file.Read(headerBuf); err != nil {
		goto E
	}

	header, _ = parseEventHeader(headerBuf)

	// read the event body
	bodyBuf = make([]byte, header.size-19)
	_, err = fr.file.Read(bodyBuf)
	if err != nil {
		goto E
	}

	// combine both the buffers
	eventBuf = make([]byte, header.size)
	copy(eventBuf, headerBuf)
	copy(eventBuf[19:], bodyBuf)

	fr.nextEvent = eventBuf
	return nil

E:
	if err == io.EOF {
		return err
	} else {
		return myError(ErrFile, err)
	}
}

func (fr *fileReader) error() error {
	return fr.e
}

func parseString2(b []byte, length uint16) (string, int) {
	if length < 256 {
		length = uint16(b[0])
		return string(b[1 : 1+length]), int(length) + 1
	} else {
		length = parseUint16(b)
		return string(b[2 : 2+length]), int(length) + 2
	}
}
