package mysql

import (
	"net/url"
	"strconv"
	"strings"
)

// default properties (unexported)
const (
	_DEFAULT_HOST            = "127.0.0.1"
	_DEFAULT_PORT            = "3306"
	_DEFAULT_MAX_PACKET_SIZE = 16 * 1024 * 1024 // 16MB
	_DEFAULT_SLAVE_ID        = 0
	_DEFAULT_CAPABILITIES    = (_CLIENT_LONG_PASSWORD |
		_CLIENT_LONG_FLAG |
		_CLIENT_TRANSACTIONS |
		_CLIENT_PROTOCOL41 |
		_CLIENT_SECURE_CONNECTION |
		_CLIENT_MULTI_RESULTS |
		_CLIENT_PLUGIN_AUTH)
)

type properties struct {
	scheme             string // mysql or file (for binlog files)
	file               string // file://<binlog file>
	username           string
	password           string
	passwordSet        bool
	address            string // host:port
	schema             string
	socket             string
	clientCapabilities uint32
	maxPacketSize      uint32

	sslCA   string
	sslCert string
	sslKey  string

	slaveId        uint32 // used while registering as slave
	reportWarnings bool   // report warnings count as error
}

func (p *properties) parseUrl(dsn string) error {
	// initialize default properties
	p.clientCapabilities = _DEFAULT_CAPABILITIES

	u, err := url.Parse(dsn)
	if err != nil {
		return err
	}

	p.scheme = u.Scheme

	if p.scheme == "file" {
		p.file = u.Path
		return nil
	}

	if u.User != nil {
		p.username = u.User.Username()
		p.password, p.passwordSet = u.User.Password()
	}
	p.address = parseHost(u.Host)

	p.schema = strings.TrimLeft(u.Path, "/")
	if p.schema != "" {
		p.clientCapabilities |= _CLIENT_CONNECT_WITH_DB
	}

	query := u.Query()

	// Socket
	p.socket = query.Get("Socket")

	// LocalInfile
	if val := query.Get("LocalInfile"); val != "" {
		if v, err := strconv.ParseBool(val); err != nil {
			return err
		} else if v {
			p.clientCapabilities |= _CLIENT_LOCAL_FILES
		}
	}

	// MaxAllowedPacket
	if val := query.Get("MaxAllowedPacket"); val != "" {
		if v, err := strconv.ParseUint(val, 10, 32); err != nil {
			return err
		} else {
			p.maxPacketSize = uint32(v)
		}
	} else {
		p.maxPacketSize = _DEFAULT_MAX_PACKET_SIZE
	}

	// SSLCA
	if val := query.Get("SSLCA"); val != "" {
		p.sslCA = val
		p.clientCapabilities |= _CLIENT_SSL
	}

	// SSLCert
	if val := query.Get("SSLCert"); val != "" {
		p.sslCert = val
		p.clientCapabilities |= _CLIENT_SSL
	}

	// SSLKey
	if val := query.Get("SSLKey"); val != "" {
		p.sslKey = val
		p.clientCapabilities |= _CLIENT_SSL
	}

	// Compress
	if val := query.Get("Compress"); val != "" {
		if v, err := strconv.ParseBool(val); err != nil {
			return err
		} else if v {
			p.clientCapabilities |= _CLIENT_COMPRESS
		}
	}

	// SlaveId
	if val := query.Get("SlaveId"); val != "" {
		if v, err := strconv.ParseUint(val, 10, 32); err != nil {
			return err
		} else {
			p.slaveId = uint32(v)
		}
	} else {
		p.slaveId = _DEFAULT_SLAVE_ID
	}

	// ReportWarnings
	if val := query.Get("ReportWarnings"); val != "" {
		if v, err := strconv.ParseBool(val); err != nil {
			return err
		} else {
			p.reportWarnings = v
		}
	}
	return nil
}

// parseHost returns the address in 'host:port' format. default ip (127.0.0.1) and
// port (3306) are used if not specified.
func parseHost(addr string) string {
	var (
		host, port      string
		defaultAssigned bool
	)

	v := strings.Split(addr, ":")

	switch len(v) {
	case 2:
		host = v[0]
		port = v[1]

		if host == "" {
			host = _DEFAULT_HOST
			defaultAssigned = true
		}

		if port == "" {
			port = _DEFAULT_PORT
			defaultAssigned = true
		}

		if defaultAssigned == false {
			return addr // addr is already in required format
		}
		break

	case 1:
		host = v[0]
		if host == "" {
			host = _DEFAULT_HOST
		}
		port = _DEFAULT_PORT
	case 0:
		fallthrough
	default:
		host = _DEFAULT_HOST
		port = _DEFAULT_PORT
		break
	}
	return strings.Join([]string{host, port}, ":")
}
