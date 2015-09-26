package mysql

import (
	"encoding/binary"
	"fmt"
	"time"
)

const (
	UNKNOWN_EVENT = iota
	START_EVENT_V3
	QUERY_EVENT
	STOP_EVENT
	ROTATE_EVENT
	INTVAR_EVENT
	LOAD_EVENT
	SLAVE_EVENT
	CREATE_FILE_EVENT
	APPEND_BLOCK_EVENT
	EXEC_LOAD_EVENT
	DELETE_FILE_EVENT
	NEW_LOAD_EVENT
	RAND_EVENT
	USER_VAR_EVENT
	FORMAT_DESCRIPTION_EVENT
	XID_EVENT
	BEGIN_LOAD_QUERY_EVENT
	EXECUTE_LOAD_QUERY_EVENT
	TABLE_MAP_EVENT
	PRE_GA_WRITE_ROWS_EVENT
	PRE_GA_UPDATE_ROWS_EVENT
	PRE_GA_DELETE_ROWS_EVENT
	WRITE_ROWS_EVENT_V1
	UPDATE_ROWS_EVENT_V1
	DELETE_ROWS_EVENT_V1
	INCIDENT_EVENT
	HEARTBEAT_LOG_EVENT
	IGNORABLE_LOG_EVENT
	ROWS_QUERY_LOG_EVENT
	WRITE_ROWS_EVENT
	UPDATE_ROWS_EVENT
	DELETE_ROWS_EVENT
	GTID_LOG_EVENT
	ANONYMOUS_GTID_LOG_EVENT
	PREVIOUS_GTIDS_LOG_EVENT
	TRANSACTION_CONTEXT_EVENT
	VIEW_CHANGE_EVENT
	XA_PREPARE_LOG_EVENT

	// new Oracle MySQL events should go right above this comment
	_ // MYSQL_EVENTS_END
	_ // MARIA_EVENTS_BEGIN

	// MariaDB specific event numbers start from here
	ANNOTATE_ROWS_EVENT     = 160
	BINLOG_CHECKPOINT_EVENT = 161
	GTID_EVENT              = 162
	GTID_LIST_EVENT         = 163
)

// Binlog represents the binlog context
type Binlog struct {
	reader   binlogReader
	index    binlogIndex
	desc     eventDescription
	tableMap *TableMapEvent
}

type binlogReader interface {
	begin(index binlogIndex) error
	close() error
	next() bool
	event() []byte
}

// received from format descriptor event
type eventDescription struct {
	binlogVersion      uint16
	serverVersion      string
	creationTime       time.Time
	commonHeaderLength uint8
	postHeaderLength   []byte
}

type binlogIndex struct {
	position uint32
	file     string
	// TODO: add GTID support
}

func (b *Binlog) Connect(dsn string) error {
	var (
		err error
		p   properties
	)

	// parse the dsn
	if err = p.parseUrl(dsn); err != nil {
		return err
	}

	switch p.scheme {
	case "mysql":
		nr := new(netReader)

		// initialize netReader
		if err = nr.init(p); err != nil {
			return err
		} else {
			b.reader = nr
		}

	case "file":
		fr := new(fileReader)
		if err = fr.init(p); err != nil {
			return err
		} else {
			b.reader = fr
		}

	default:
		return myError(ErrScheme, p.scheme)

	}
	return nil
}

func (b *Binlog) SetPosition(position uint32) {
	b.index.position = position
}

func (b *Binlog) SetFile(file string) {
	b.index.file = file
}

func (b *Binlog) Begin() error {
	return b.reader.begin(b.index)
}

func (b *Binlog) Next() bool {
	return b.reader.next()
}

func (b *Binlog) RawEvent() (re RawEvent) {
	var off int

	re.body = b.reader.event()
	re.header, off = parseEventHeader(re.body)

	switch re.header.type_ {
	case START_EVENT_V3:
		ev := new(StartEventV3)
		b.parseStartEventV3(re.body[off:], ev)

		// now that we have parsed START_EVENT_V3, we can
		// update binlog description
		b.desc.binlogVersion = ev.binlogVersion
		b.desc.serverVersion = ev.serverVersion
		b.desc.creationTime = ev.creationTime
	case FORMAT_DESCRIPTION_EVENT:
		ev := new(FormatDescriptionEvent)
		b.parseFormatDescriptionEvent(re.body[off:], ev)

		// now that we have parsed FORMAT_DESCRIPTION_EVENT, we can
		// update binlog description
		b.desc.binlogVersion = ev.binlogVersion
		b.desc.serverVersion = ev.serverVersion
		b.desc.creationTime = ev.creationTime
		b.desc.commonHeaderLength = ev.commonHeaderLength
		// number of events
		b.desc.postHeaderLength = make([]byte, len(ev.postHeaderLength))
		copy(b.desc.postHeaderLength, ev.postHeaderLength)
	default: // do nothing

	}
	re.binlog = b
	return
}

func (b *Binlog) Close() error {
	return b.reader.close()
}

type eventHeader struct {
	timestamp uint32
	type_     uint8
	serverId  uint32
	size      uint32
	position  uint32
	flags     uint16
}

type Event interface {
	Time() time.Time
	Type() uint8
	ServerId() uint32
	Size() uint32
	Position() uint32
}

type RawEvent struct {
	header eventHeader
	binlog *Binlog
	body   []byte
}

func (e *RawEvent) Time() time.Time {
	return time.Unix(int64(e.header.timestamp), 0)
}

func (e *RawEvent) Type() uint8 {
	return e.header.type_
}

func (e *RawEvent) Name() string {
	return eventName(e.header.type_)
}

func (e *RawEvent) ServerId() uint32 {
	return e.header.serverId
}

func (e *RawEvent) Size() uint32 {
	return e.header.size
}

func (e *RawEvent) Position() uint32 {
	return e.header.position
}

func (e *RawEvent) Body() []byte {
	return e.body
}

func (re *RawEvent) Event() Event {
	binlog := re.binlog
	header := re.header
	buf := re.body

	// move past event header, as it has already been parsed
	off := 19

	switch re.header.type_ {
	case START_EVENT_V3:
		ev := new(StartEventV3)
		ev.header = re.header

		/*
		   no need to parse the payload, it has already been parsed in
		   RawEvent().
		*/
		desc := re.binlog.desc
		ev.binlogVersion = desc.binlogVersion
		ev.serverVersion = desc.serverVersion
		ev.creationTime = desc.creationTime
		return ev

	case QUERY_EVENT:
		ev := new(QueryEvent)
		ev.header = header
		binlog.parseQueryEvent(buf[off:], ev)
		return ev

	case STOP_EVENT:
		ev := new(StopEvent)
		ev.header = header
		// STOP_EVENT has no post-header or payload
		return ev

	case ROTATE_EVENT:
		ev := new(RotateEvent)
		ev.header = header
		binlog.parseRotateEvent(buf[off:], ev)
		return ev

	case INTVAR_EVENT:
		ev := new(IntvarEvent)
		ev.header = header
		binlog.parseIntvarEvent(buf[off:], ev)
		return ev

	case LOAD_EVENT:
		fallthrough
	case NEW_LOAD_EVENT:
		ev := new(LoadEvent)
		ev.header = header
		binlog.parseLoadEvent(buf[off:], ev)
		return ev

	case SLAVE_EVENT:
		ev := new(SlaveEvent)
		ev.header = header
		binlog.parseSlaveEvent(buf[off:], ev)
		return ev

	case CREATE_FILE_EVENT:
		ev := new(CreateFileEvent)
		ev.header = header
		binlog.parseCreateFileEvent(buf[off:], ev)
		return ev

	case APPEND_BLOCK_EVENT:
		ev := new(AppendBlockEvent)
		ev.header = header
		binlog.parseAppendBlockEvent(buf[off:], ev)
		return ev

	case EXEC_LOAD_EVENT:
		ev := new(ExecLoadEvent)
		ev.header = header
		binlog.parseExecLoadEvent(buf[off:], ev)
		return ev

	case DELETE_FILE_EVENT:
		ev := new(DeleteFileEvent)
		ev.header = header
		binlog.parseDeleteFileEvent(buf[off:], ev)
		return ev

	case RAND_EVENT:
		ev := new(RandEvent)
		ev.header = header
		binlog.parseRandEvent(buf[off:], ev)
		return ev

	case USER_VAR_EVENT:
		ev := new(UserVarEvent)
		ev.header = header
		binlog.parseUserVarEvent(buf[off:], ev)
		return ev

	case FORMAT_DESCRIPTION_EVENT:
		ev := new(FormatDescriptionEvent)
		ev.header = header

		/*
		   no need to parse the payload, it has already been parsed in
		   RawEvent().
		*/
		desc := re.binlog.desc
		ev.binlogVersion = desc.binlogVersion
		ev.serverVersion = desc.serverVersion
		ev.creationTime = desc.creationTime
		ev.commonHeaderLength = desc.commonHeaderLength
		// number of events
		ev.postHeaderLength = make([]byte, len(desc.postHeaderLength))
		copy(ev.postHeaderLength, desc.postHeaderLength)
		return ev

	case XID_EVENT:
		ev := new(XidEvent)
		ev.header = header
		binlog.parseXidEvent(buf[off:], ev)
		return ev

	case BEGIN_LOAD_QUERY_EVENT:
		ev := new(BeginLoadQueryEvent)
		ev.header = header
		binlog.parseBeginLoadQueryEvent(buf[off:], ev)
		return ev

	case EXECUTE_LOAD_QUERY_EVENT:
		ev := new(ExecuteLoadQueryEvent)
		ev.header = header
		binlog.parseExecuteLoadQueryEvent(buf[off:], ev)
		return ev

	case TABLE_MAP_EVENT:
		ev := new(TableMapEvent)
		ev.header = header
		binlog.parseTableMapEvent(buf[off:], ev)
		binlog.tableMap = ev
		return ev

	case PRE_GA_UPDATE_ROWS_EVENT, UPDATE_ROWS_EVENT_V1,
		UPDATE_ROWS_EVENT, PRE_GA_WRITE_ROWS_EVENT,
		WRITE_ROWS_EVENT_V1, WRITE_ROWS_EVENT,
		PRE_GA_DELETE_ROWS_EVENT, DELETE_ROWS_EVENT_V1,
		DELETE_ROWS_EVENT:
		ev := new(RowsEvent)
		ev.header = header
		binlog.parseRowsEvent(buf[off:], ev)
		return ev

	case INCIDENT_EVENT:
		ev := new(IncidentEvent)
		ev.header = header
		binlog.parseIncidentEvent(buf[off:], ev)
		return ev

	case HEARTBEAT_LOG_EVENT:
		ev := new(HeartbeatLogEvent)
		ev.header = header
		return ev

	case IGNORABLE_LOG_EVENT:
		ev := new(IgnorableLogEvent)
		ev.header = header
		return ev

	case ROWS_QUERY_LOG_EVENT:
		ev := new(RowsQueryLogEvent)
		ev.header = header
		binlog.parseRowsQueryLogEvent(buf[off:], ev)
		return ev

	case ANNOTATE_ROWS_EVENT:
		ev := new(AnnotateRowsEvent)
		ev.header = header
		binlog.parseAnnotateRowsEvent(buf[off:], ev)
		return ev

	case BINLOG_CHECKPOINT_EVENT:
		ev := new(BinlogCheckpointEvent)
		ev.header = header
		binlog.parseBinlogCheckpointEvent(buf[off:], ev)
		return ev

	case GTID_EVENT:
		ev := new(GtidEvent)
		ev.header = header
		binlog.parseGtidEvent(buf[off:], ev)
		return ev

	case GTID_LIST_EVENT:
		ev := new(GtidListEvent)
		ev.header = header
		binlog.parseGtidListEvent(buf[off:], ev)
		return ev

	default: // unimplemented events
	}
	return nil
}

// QUERY_EVENT
type QueryEvent struct {
	header        eventHeader
	slaveProxyId  uint32
	executionTime time.Time
	errorCode     uint16
	schema        string
	query         string
	statusVars    string
}

func (e *QueryEvent) Time() time.Time {
	return time.Unix(int64(e.header.timestamp), 0)
}

func (e *QueryEvent) Type() uint8 {
	return e.header.type_
}

func (e *QueryEvent) ServerId() uint32 {
	return e.header.serverId
}

func (e *QueryEvent) Size() uint32 {
	return e.header.size
}

func (e *QueryEvent) Position() uint32 {
	return e.header.position
}

func (e *QueryEvent) SlaveProxyId() uint32 {
	return e.slaveProxyId
}

func (e *QueryEvent) ExecutionTime() time.Time {
	return e.executionTime
}

func (e *QueryEvent) Error() uint16 {
	return e.errorCode
}

func (e *QueryEvent) Schema() string {
	return e.schema
}

func (e *QueryEvent) Query() string {
	return e.query
}

func (e *QueryEvent) StatusVars() string {
	return e.statusVars
}

const UNSIGNED = 1

// USER_VAR_EVENT
type UserVarEvent struct {
	header  eventHeader
	name    string
	null    bool
	type_   uint8
	charset uint32
	value   []byte
	flags   uint8
}

func (e *UserVarEvent) Time() time.Time {
	return time.Unix(int64(e.header.timestamp), 0)
}

func (e *UserVarEvent) Type() uint8 {
	return e.header.type_
}

func (e *UserVarEvent) ServerId() uint32 {
	return e.header.serverId
}

func (e *UserVarEvent) Size() uint32 {
	return e.header.size
}

func (e *UserVarEvent) Position() uint32 {
	return e.header.position
}

func (e *UserVarEvent) Name() string {
	return e.name
}

func (e *UserVarEvent) Value() interface{} {
	var unsigned bool

	if e.null {
		return nil
	}

	if (e.flags & uint8(UNSIGNED)) != 0 {
		unsigned = true
	}

	switch e.type_ {
	// string
	case _TYPE_STRING, _TYPE_VARCHAR,
		_TYPE_VARSTRING, _TYPE_ENUM,
		_TYPE_SET, _TYPE_BLOB,
		_TYPE_TINY_BLOB, _TYPE_MEDIUM_BLOB,
		_TYPE_LONG_BLOB, _TYPE_GEOMETRY,
		_TYPE_BIT, _TYPE_DECIMAL,
		_TYPE_NEW_DECIMAL:
		v, _ := parseString(e.value)
		return v

	// int64/uint64
	case _TYPE_LONG_LONG:
		if unsigned {
			return binary.LittleEndian.Uint64(e.value)
		} else {
			return parseInt64(e.value)
		}

	// int32/uint32
	case _TYPE_LONG, _TYPE_INT24:
		if unsigned {
			return binary.LittleEndian.Uint32(e.value)
		} else {
			return parseInt32(e.value)
		}

		// int16/uint6
	case _TYPE_SHORT:
		if unsigned {
			return binary.LittleEndian.Uint16(e.value)
		} else {
			return parseInt16(e.value)
		}

	// uint16
	case _TYPE_YEAR:
		return binary.LittleEndian.Uint16(e.value)

	// int8
	case _TYPE_TINY:
		if unsigned {
			return uint8(e.value[0])
		} else {
			return int8(e.value[0])
		}

	// float64
	case _TYPE_DOUBLE:
		return parseDouble(e.value)

	// float32
	case _TYPE_FLOAT:
		return parseFloat(e.value)

	// time.Time
	case _TYPE_DATE, _TYPE_DATETIME,
		_TYPE_TIMESTAMP:
		v, _ := parseDate(e.value)
		return v

	// time.Duration
	case _TYPE_TIME:
		v, _ := parseTime(e.value)
		return v

	// TODO: map the following unhandled types accordingly
	case _TYPE_NEW_DATE, _TYPE_TIMESTAMP2,
		_TYPE_DATETIME2, _TYPE_TIME2,
		_TYPE_NULL:
		fallthrough
	default:
	}
	return nil
}

// FORMAT_DESCRIPTION_EVENT
type FormatDescriptionEvent struct {
	header             eventHeader
	binlogVersion      uint16
	serverVersion      string
	creationTime       time.Time
	commonHeaderLength uint8
	postHeaderLength   []byte
}

func (e *FormatDescriptionEvent) Time() time.Time {
	return time.Unix(int64(e.header.timestamp), 0)
}

func (e *FormatDescriptionEvent) Type() uint8 {
	return e.header.type_
}

func (e *FormatDescriptionEvent) ServerId() uint32 {
	return e.header.serverId
}

func (e *FormatDescriptionEvent) Size() uint32 {
	return e.header.size
}

func (e *FormatDescriptionEvent) Position() uint32 {
	return e.header.position
}

func (e *FormatDescriptionEvent) BinlogVersion() uint16 {
	return e.binlogVersion
}

func (e *FormatDescriptionEvent) ServerVersion() string {
	return e.serverVersion
}

func (e *FormatDescriptionEvent) CreationTime() time.Time {
	return e.creationTime
}

// STOP_EVENT
type StopEvent struct {
	header eventHeader
}

func (e *StopEvent) Time() time.Time {
	return time.Unix(int64(e.header.timestamp), 0)
}

func (e *StopEvent) Type() uint8 {
	return e.header.type_
}

func (e *StopEvent) ServerId() uint32 {
	return e.header.serverId
}

func (e *StopEvent) Size() uint32 {
	return e.header.size
}

func (e *StopEvent) Position() uint32 {
	return e.header.position
}

// ROTATE_EVENT
type RotateEvent struct {
	header   eventHeader
	position uint64
	file     string
}

func (e *RotateEvent) Time() time.Time {
	return time.Unix(int64(e.header.timestamp), 0)
}

func (e *RotateEvent) Type() uint8 {
	return e.header.type_
}

func (e *RotateEvent) ServerId() uint32 {
	return e.header.serverId
}

func (e *RotateEvent) Size() uint32 {
	return e.header.size
}

func (e *RotateEvent) Position() uint32 {
	return e.header.position
}

func (e *RotateEvent) NextFile() string {
	return e.file
}

func (e *RotateEvent) NextPosition() uint64 {
	return e.position
}

// START_EVENT_V3
type StartEventV3 struct {
	header        eventHeader
	binlogVersion uint16
	serverVersion string
	creationTime  time.Time
}

func (e *StartEventV3) Time() time.Time {
	return time.Unix(int64(e.header.timestamp), 0)
}

func (e *StartEventV3) Type() uint8 {
	return e.header.type_
}

func (e *StartEventV3) ServerId() uint32 {
	return e.header.serverId
}

func (e *StartEventV3) Size() uint32 {
	return e.header.size
}

func (e *StartEventV3) Position() uint32 {
	return e.header.position
}

func (e *StartEventV3) BinlogVersion() uint16 {
	return e.binlogVersion
}

func (e *StartEventV3) ServerVersion() string {
	return e.serverVersion
}

func (e *StartEventV3) CreationTime() time.Time {
	return e.creationTime
}

// HEARTBEAT_LOG_EVENT
type HeartbeatLogEvent struct {
	header eventHeader
}

func (e *HeartbeatLogEvent) Time() time.Time {
	return time.Unix(int64(e.header.timestamp), 0)
}

func (e *HeartbeatLogEvent) Type() uint8 {
	return e.header.type_
}

func (e *HeartbeatLogEvent) ServerId() uint32 {
	return e.header.serverId
}

func (e *HeartbeatLogEvent) Size() uint32 {
	return e.header.size
}

func (e *HeartbeatLogEvent) Position() uint32 {
	return e.header.position
}

// IGNORABLE_LOG_EVENT
type IgnorableLogEvent struct {
	header eventHeader
}

func (e *IgnorableLogEvent) Time() time.Time {
	return time.Unix(int64(e.header.timestamp), 0)
}

func (e *IgnorableLogEvent) Type() uint8 {
	return e.header.type_
}

func (e *IgnorableLogEvent) ServerId() uint32 {
	return e.header.serverId
}

func (e *IgnorableLogEvent) Size() uint32 {
	return e.header.size
}

func (e *IgnorableLogEvent) Position() uint32 {
	return e.header.position
}

// ROWS_QUERY_LOG_EVENT
type RowsQueryLogEvent struct {
	header eventHeader
	query  string
}

func (e *RowsQueryLogEvent) Time() time.Time {
	return time.Unix(int64(e.header.timestamp), 0)
}

func (e *RowsQueryLogEvent) Type() uint8 {
	return e.header.type_
}

func (e *RowsQueryLogEvent) ServerId() uint32 {
	return e.header.serverId
}

func (e *RowsQueryLogEvent) Size() uint32 {
	return e.header.size
}

func (e *RowsQueryLogEvent) Position() uint32 {
	return e.header.position
}

func (e *RowsQueryLogEvent) Query() string {
	return e.query
}

// XID_EVENT
type XidEvent struct {
	header eventHeader
	xid    uint64
}

func (e *XidEvent) Time() time.Time {
	return time.Unix(int64(e.header.timestamp), 0)
}

func (e *XidEvent) Type() uint8 {
	return e.header.type_
}

func (e *XidEvent) ServerId() uint32 {
	return e.header.serverId
}

func (e *XidEvent) Size() uint32 {
	return e.header.size
}

func (e *XidEvent) Position() uint32 {
	return e.header.position
}

func (e *XidEvent) Xid() uint64 {
	return e.xid
}

const (
	INCIDENT_NONE        = 0
	INCIDENT_LOST_EVENTS = 1
)

// INCIDENT_EVENT
type IncidentEvent struct {
	header  eventHeader
	type_   uint16
	message string
}

func (e *IncidentEvent) Time() time.Time {
	return time.Unix(int64(e.header.timestamp), 0)
}

func (e *IncidentEvent) Type() uint8 {
	return e.header.type_
}

func (e *IncidentEvent) ServerId() uint32 {
	return e.header.serverId
}

func (e *IncidentEvent) Size() uint32 {
	return e.header.size
}

func (e *IncidentEvent) Position() uint32 {
	return e.header.position
}

func (e *IncidentEvent) IncidentType() uint16 {
	return e.type_
}

func (e *IncidentEvent) IncidentMessage() string {
	return e.message
}

// RAND_EVENT
type RandEvent struct {
	header eventHeader
	seed1  uint64
	seed2  uint64
}

func (e *RandEvent) Time() time.Time {
	return time.Unix(int64(e.header.timestamp), 0)
}

func (e *RandEvent) Type() uint8 {
	return e.header.type_
}

func (e *RandEvent) ServerId() uint32 {
	return e.header.serverId
}

func (e *RandEvent) Size() uint32 {
	return e.header.size
}

func (e *RandEvent) Position() uint32 {
	return e.header.position
}

func (e *RandEvent) Seed1() uint64 {
	return e.seed1
}

func (e *RandEvent) Seed2() uint64 {
	return e.seed2
}

const (
	INVALID_INT_EVENT = iota
	LAST_INSERT_ID_EVENT
	INSERT_ID_EVENT
)

// INTVAR_EVENT
type IntvarEvent struct {
	header eventHeader
	type_  uint8
	value  uint64
}

func (e *IntvarEvent) Time() time.Time {
	return time.Unix(int64(e.header.timestamp), 0)
}

func (e *IntvarEvent) Type() uint8 {
	return e.header.type_
}

func (e *IntvarEvent) ServerId() uint32 {
	return e.header.serverId
}

func (e *IntvarEvent) Size() uint32 {
	return e.header.size
}

func (e *IntvarEvent) Position() uint32 {
	return e.header.position
}

func (e *IntvarEvent) IntvarType() uint8 {
	return e.type_
}

func (e *IntvarEvent) Value() uint64 {
	return e.value
}

// OptFlags
const (
	DUMPFILE_FLAG = 1 << iota
	OPT_ENCLOSED_FLAG
	REPLAVE_FLAG
	IGNORE_FLAG
)

// EmptyFlags
const (
	FIELD_TERM_EMPTY = 1 << iota
	ENCLOSED_EMPTY
	LINE_TERM_EMPTY
	LINE_START_EMPTY
	ESCAPE_EMPTY
)

// LOAD_EVENT
type LoadEvent struct {
	header          eventHeader
	slaveProxyId    uint32
	executionTime   time.Time
	skipLines       uint32
	fieldCount      uint32
	fieldTerminator string
	enclosedBy      string
	lineTerminator  string
	lineStart       string
	escapedBy       string
	optFlags        []byte
	emptyFlags      uint8
	fields          []string
	table           string
	schema          string
	file            string
}

func (e *LoadEvent) Time() time.Time {
	return time.Unix(int64(e.header.timestamp), 0)
}

func (e *LoadEvent) Type() uint8 {
	return e.header.type_
}

func (e *LoadEvent) ServerId() uint32 {
	return e.header.serverId
}

func (e *LoadEvent) Size() uint32 {
	return e.header.size
}

func (e *LoadEvent) Position() uint32 {
	return e.header.position
}

func (e *LoadEvent) SlaveProxyId() uint32 {
	return e.slaveProxyId
}

func (e *LoadEvent) ExecutionTime() time.Time {
	return e.executionTime
}

func (e *LoadEvent) SkipLines() uint32 {
	return e.skipLines
}

func (e *LoadEvent) FieldCount() uint32 {
	return e.fieldCount
}

func (e *LoadEvent) FieldTerminator() string {
	return e.fieldTerminator
}

func (e *LoadEvent) EnclosedBy() string {
	return e.enclosedBy
}

func (e *LoadEvent) LineTerminator() string {
	return e.lineTerminator
}

func (e *LoadEvent) LineStart() string {
	return e.lineStart
}

func (e *LoadEvent) EscapedBy() string {
	return e.escapedBy
}

func (e *LoadEvent) OptFlags() []byte {
	return e.optFlags
}

func (e *LoadEvent) EmptyFlags() uint8 {
	return e.emptyFlags
}

func (e *LoadEvent) Fields() []string {
	return e.fields
}

func (e *LoadEvent) Table() string {
	return e.table
}

func (e *LoadEvent) Schema() string {
	return e.schema
}

func (e *LoadEvent) File() string {
	return e.file
}

// SLAVE_EVENT
type SlaveEvent struct {
	header         eventHeader
	masterPosition uint64
	masterPort     uint16
	masterHost     string
	masterLog      string
}

func (e *SlaveEvent) Time() time.Time {
	return time.Unix(int64(e.header.timestamp), 0)
}

func (e *SlaveEvent) Type() uint8 {
	return e.header.type_
}

func (e *SlaveEvent) ServerId() uint32 {
	return e.header.serverId
}

func (e *SlaveEvent) Size() uint32 {
	return e.header.size
}

func (e *SlaveEvent) Position() uint32 {
	return e.header.position
}

func (e *SlaveEvent) MasterPosition() uint64 {
	return e.masterPosition
}

func (e *SlaveEvent) MasterPort() uint16 {
	return e.masterPort
}

func (e *SlaveEvent) MasterHost() string {
	return e.masterHost
}

func (e *SlaveEvent) MasterLog() string {
	return e.masterLog
}

// CREATE_FILE_EVENT
type CreateFileEvent struct {
	header eventHeader
	fileId uint32
	data   []byte
}

func (e *CreateFileEvent) Time() time.Time {
	return time.Unix(int64(e.header.timestamp), 0)
}

func (e *CreateFileEvent) Type() uint8 {
	return e.header.type_
}

func (e *CreateFileEvent) ServerId() uint32 {
	return e.header.serverId
}

func (e *CreateFileEvent) Size() uint32 {
	return e.header.size
}

func (e *CreateFileEvent) Position() uint32 {
	return e.header.position
}

func (e *CreateFileEvent) FileId() uint32 {
	return e.fileId
}

func (e *CreateFileEvent) Data() []byte {
	return e.data
}

// DELETE_FILE_EVENT
type DeleteFileEvent struct {
	header eventHeader
	fileId uint32
}

func (e *DeleteFileEvent) Time() time.Time {
	return time.Unix(int64(e.header.timestamp), 0)
}

func (e *DeleteFileEvent) Type() uint8 {
	return e.header.type_
}

func (e *DeleteFileEvent) ServerId() uint32 {
	return e.header.serverId
}

func (e *DeleteFileEvent) Size() uint32 {
	return e.header.size
}

func (e *DeleteFileEvent) Position() uint32 {
	return e.header.position
}

func (e *DeleteFileEvent) FileId() uint32 {
	return e.fileId
}

// APPEND_BLOCK_EVENT
type AppendBlockEvent struct {
	header  eventHeader
	fieldId uint32
	data    []byte
}

func (e *AppendBlockEvent) Time() time.Time {
	return time.Unix(int64(e.header.timestamp), 0)
}

func (e *AppendBlockEvent) Type() uint8 {
	return e.header.type_
}

func (e *AppendBlockEvent) ServerId() uint32 {
	return e.header.serverId
}

func (e *AppendBlockEvent) Size() uint32 {
	return e.header.size
}

func (e *AppendBlockEvent) Position() uint32 {
	return e.header.position
}

func (e *AppendBlockEvent) FieldId() uint32 {
	return e.fieldId
}

func (e *AppendBlockEvent) Data() []byte {
	return e.data
}

// EXEC_LOAD_EVENT
type ExecLoadEvent struct {
	header eventHeader
	fileId uint32
}

func (e *ExecLoadEvent) Time() time.Time {
	return time.Unix(int64(e.header.timestamp), 0)
}

func (e *ExecLoadEvent) Type() uint8 {
	return e.header.type_
}

func (e *ExecLoadEvent) ServerId() uint32 {
	return e.header.serverId
}

func (e *ExecLoadEvent) Size() uint32 {
	return e.header.size
}

func (e *ExecLoadEvent) Position() uint32 {
	return e.header.position
}

func (e *ExecLoadEvent) FileId() uint32 {
	return e.fileId
}

// BEGIN_LOAD_QUERY_EVENT
type BeginLoadQueryEvent struct {
	header eventHeader
	fileId uint32
	data   []byte
}

func (e *BeginLoadQueryEvent) Time() time.Time {
	return time.Unix(int64(e.header.timestamp), 0)
}

func (e *BeginLoadQueryEvent) Type() uint8 {
	return e.header.type_
}

func (e *BeginLoadQueryEvent) ServerId() uint32 {
	return e.header.serverId
}

func (e *BeginLoadQueryEvent) Size() uint32 {
	return e.header.size
}

func (e *BeginLoadQueryEvent) Position() uint32 {
	return e.header.position
}

func (e *BeginLoadQueryEvent) FileId() uint32 {
	return e.fileId
}

func (e *BeginLoadQueryEvent) Data() []byte {
	return e.data
}

// EXECUTE_LOAD_QUERY_EVENT
type ExecuteLoadQueryEvent struct {
	header           eventHeader
	slaveProxyId     uint32
	executionTime    time.Time
	schemaLength     uint8
	errorCode        uint16
	statusVarsLength uint16
	fileId           uint32
	startPosition    uint32
	endPosition      uint32
	dupHandlingFlags uint8
}

func (e *ExecuteLoadQueryEvent) Time() time.Time {
	return time.Unix(int64(e.header.timestamp), 0)
}

func (e *ExecuteLoadQueryEvent) Type() uint8 {
	return e.header.type_
}

func (e *ExecuteLoadQueryEvent) ServerId() uint32 {
	return e.header.serverId
}

func (e *ExecuteLoadQueryEvent) Size() uint32 {
	return e.header.size
}

func (e *ExecuteLoadQueryEvent) Position() uint32 {
	return e.header.position
}

type EventColumns struct {
	columnCount uint16
	columns     []*EventColumn

	// iterator
	pos    uint64
	closed bool
}

type EventColumn struct {
	type_    uint8
	meta     uint16
	nullable bool
}

type TableMapEvent struct {
	header      eventHeader
	tableId     uint64
	flags       uint16
	schema      string
	table       string
	columnCount uint64
	columns     []EventColumn
}

func (e *TableMapEvent) Time() time.Time {
	return time.Unix(int64(e.header.timestamp), 0)
}

func (e *TableMapEvent) Type() uint8 {
	return e.header.type_
}

func (e *TableMapEvent) ServerId() uint32 {
	return e.header.serverId
}

func (e *TableMapEvent) Size() uint32 {
	return e.header.size
}

func (e *TableMapEvent) Position() uint32 {
	return e.header.position
}

func (e *TableMapEvent) TableId() uint64 {
	return e.tableId
}

func (e *TableMapEvent) Flags() uint16 {
	return e.flags
}

func (e *TableMapEvent) Schema() string {
	return e.schema
}

func (e *TableMapEvent) Table() string {
	return e.table
}

func (e *TableMapEvent) ColumnCount() uint64 {
	return e.columnCount
}

type RowsEvent struct {
	header                eventHeader
	tableId               uint64
	flags                 uint16
	extraData             []byte
	columnCount           uint64
	columnsPresentBitmap1 []byte
	columnsPresentBitmap2 []byte
	rows1                 EventRows
	rows2                 EventRows
}

func (e *RowsEvent) Time() time.Time {
	return time.Unix(int64(e.header.timestamp), 0)
}

func (e *RowsEvent) Type() uint8 {
	return e.header.type_
}

func (e *RowsEvent) ServerId() uint32 {
	return e.header.serverId
}

func (e *RowsEvent) Size() uint32 {
	return e.header.size
}

func (e *RowsEvent) Position() uint32 {
	return e.header.position
}

func (e *RowsEvent) Image() EventRows {
	return e.rows1
}

func (e *RowsEvent) AfterImage() EventRows {
	return e.rows1
}

type EventRows struct {
	rows []EventRow

	// iterator
	pos    uint64
	closed bool
}

type EventRow struct {
	columns []interface{}
}

type AnnotateRowsEvent struct {
	header eventHeader
	query  string
}

func (e *AnnotateRowsEvent) Time() time.Time {
	return time.Unix(int64(e.header.timestamp), 0)
}

func (e *AnnotateRowsEvent) Type() uint8 {
	return e.header.type_
}

func (e *AnnotateRowsEvent) ServerId() uint32 {
	return e.header.serverId
}

func (e *AnnotateRowsEvent) Size() uint32 {
	return e.header.size
}

func (e *AnnotateRowsEvent) Position() uint32 {
	return e.header.position
}

func (e *AnnotateRowsEvent) Query() string {
	return e.query
}

type BinlogCheckpointEvent struct {
	header     eventHeader
	fileLength uint32
	file       string
}

func (e *BinlogCheckpointEvent) Time() time.Time {
	return time.Unix(int64(e.header.timestamp), 0)
}

func (e *BinlogCheckpointEvent) Type() uint8 {
	return e.header.type_
}

func (e *BinlogCheckpointEvent) ServerId() uint32 {
	return e.header.serverId
}

func (e *BinlogCheckpointEvent) Size() uint32 {
	return e.header.size
}

func (e *BinlogCheckpointEvent) Position() uint32 {
	return e.header.position
}

func (e *BinlogCheckpointEvent) FileLength() uint32 {
	return e.fileLength
}

func (e *BinlogCheckpointEvent) File() string {
	return e.file
}

const (
	FL_STANDALONE      = 1
	FL_GROUP_COMMIT_ID = 2
)

type MariadbGtid struct {
	domainId uint32
	serverId uint32
	seqno    uint64
}

type GtidEvent struct {
	header   eventHeader
	gtid     MariadbGtid
	commitId uint64
	flags    uint8
}

func (e *GtidEvent) Time() time.Time {
	return time.Unix(int64(e.header.timestamp), 0)
}

func (e *GtidEvent) Type() uint8 {
	return e.header.type_
}

func (e *GtidEvent) ServerId() uint32 {
	return e.header.serverId
}

func (e *GtidEvent) Size() uint32 {
	return e.header.size
}

func (e *GtidEvent) Position() uint32 {
	return e.header.position
}

func (e *GtidEvent) String() string {
	return fmt.Sprintf("%d:%d:%d", e.gtid.domainId, e.header.serverId,
		e.gtid.seqno)
}

func (e *GtidEvent) Seqno() uint64 {
	return e.gtid.seqno
}

func (e *GtidEvent) CommitId() uint64 {
	return e.commitId
}

func (e *GtidEvent) DomainId() uint32 {
	return e.gtid.domainId
}

func (e *GtidEvent) Flags() uint8 {
	return e.flags
}

type GtidListEvent struct {
	header eventHeader
	count  uint32
	flags  uint8
	list   []MariadbGtid
}

func (e *GtidListEvent) Time() time.Time {
	return time.Unix(int64(e.header.timestamp), 0)
}

func (e *GtidListEvent) Type() uint8 {
	return e.header.type_
}

func (e *GtidListEvent) ServerId() uint32 {
	return e.header.serverId
}

func (e *GtidListEvent) Size() uint32 {
	return e.header.size
}

func (e *GtidListEvent) Position() uint32 {
	return e.header.position
}

func (e *GtidListEvent) Count() uint32 {
	return e.count
}

func (e *GtidListEvent) List() []MariadbGtid {
	return e.list
}

func (e *GtidListEvent) Flags() uint8 {
	return e.flags
}

func eventName(type_ uint8) string {
	switch type_ {
	case START_EVENT_V3:
		return "Start_v3"
	case QUERY_EVENT:
		return "Query"
	case STOP_EVENT:
		return "Stop"
	case ROTATE_EVENT:
		return "Rotate"
	case INTVAR_EVENT:
		return "Intvar"
	case LOAD_EVENT:
		return "Load"
	case SLAVE_EVENT:
		return "Slave"
	case CREATE_FILE_EVENT:
		return "Create_file"
	case APPEND_BLOCK_EVENT:
		return "Append_block"
	case EXEC_LOAD_EVENT:
		return "Exec_load"
	case DELETE_FILE_EVENT:
		return "Delete_file"
	case NEW_LOAD_EVENT:
		return "New_load"
	case RAND_EVENT:
		return "Rand"
	case USER_VAR_EVENT:
		return "User_var"
	case FORMAT_DESCRIPTION_EVENT:
		return "Format_description"
	case XID_EVENT:
		return "Xid"
	case BEGIN_LOAD_QUERY_EVENT:
		return "Begin_load_query"
	case EXECUTE_LOAD_QUERY_EVENT:
		return "Execute_load_query"
	case TABLE_MAP_EVENT:
		return "Table_map"
	case PRE_GA_WRITE_ROWS_EVENT:
		return "Pre_ga_write_rows"
	case PRE_GA_UPDATE_ROWS_EVENT:
		return "Pre_ga_update_rows"
	case PRE_GA_DELETE_ROWS_EVENT:
		return "Pre_ga_delete_rows"
	case WRITE_ROWS_EVENT_V1:
		return "Write_rows_v1"
	case UPDATE_ROWS_EVENT_V1:
		return "Update_rows_v1"
	case DELETE_ROWS_EVENT_V1:
		return "Delete_rows_v1"
	case INCIDENT_EVENT:
		return "Incident"
	case HEARTBEAT_LOG_EVENT:
		return "Heartbeat_log"
	case IGNORABLE_LOG_EVENT:
		return "Ignorable_log"
	case ROWS_QUERY_LOG_EVENT:
		return "Rows_query_log"
	case WRITE_ROWS_EVENT:
		return "Write_rows"
	case UPDATE_ROWS_EVENT:
		return "Update_rows"
	case DELETE_ROWS_EVENT:
		return "Delete_rows"
	case GTID_LOG_EVENT:
		return "Gtid_log"
	case ANONYMOUS_GTID_LOG_EVENT:
		return "Anonymous_gtid_log"
	case PREVIOUS_GTIDS_LOG_EVENT:
		return "Previous_gtids_log"
	case TRANSACTION_CONTEXT_EVENT:
		return "Transaction_context"
	case VIEW_CHANGE_EVENT:
		return "View_change"
	case XA_PREPARE_LOG_EVENT:
		return "Xa_prepare_log"
	case ANNOTATE_ROWS_EVENT:
		return "Annotate_rows"
	case BINLOG_CHECKPOINT_EVENT:
		return "Binlog_checkpoint"
	case GTID_EVENT:
		return "Gtid"
	case GTID_LIST_EVENT:
		return "Gtid_list"
	default:
	}
	return "Unknown"
}
