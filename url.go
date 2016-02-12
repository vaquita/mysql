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
	_DEFAULT_BINLOG_VERIFY_CHECKSUM = false
)

const (
	_MAX_PACKET_SIZE_MAX = 1024 * 1024 * 1024 // 1GB
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

	reportWarnings bool // report warnings count as error

	binlogSlaveId uint32 // used while registering as slave
	// send EOF packet instead of blocking if no more events are left
	binlogDumpNonBlock bool
	// verify checksum of binary log events
	binlogVerifyChecksum bool
}

func (p *properties) parseUrl(dsn string) error {
	var (
		u   *url.URL
		err error
	)

	// initialize default properties
	p.clientCapabilities = _DEFAULT_CAPABILITIES

	if u, err = url.Parse(dsn); err != nil {
		return myError(ErrInvalidDSN, err)
	}

	// we check for its correctness later
	p.scheme = u.Scheme

	if p.scheme == "file" {
		p.file = u.Path
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
			return myError(ErrInvalidProperty, "LocalInfile", err)
		} else if v {
			p.clientCapabilities |= _CLIENT_LOCAL_FILES
		}
	}

	// MaxAllowedPacket
	if val := query.Get("MaxAllowedPacket"); val != "" {
		if v, err := strconv.ParseUint(val, 10, 32); err != nil {
			return myError(ErrInvalidProperty, "MaxAllowedPacket", err)
		} else {
			p.maxPacketSize = uint32(v)
			if p.maxPacketSize > _MAX_PACKET_SIZE_MAX {
				return myError(ErrInvalidPropertyValue, "MaxAllowedPacket",
					p.maxPacketSize)
			}
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
			return myError(ErrInvalidProperty, "Compress", err)
		} else if v {
			p.clientCapabilities |= _CLIENT_COMPRESS
		}
	}

	// BinlogSlaveId
	if val := query.Get("BinlogSlaveId"); val != "" {
		if v, err := strconv.ParseUint(val, 10, 32); err != nil {
			return myError(ErrInvalidProperty, "BinlogSlaveId", err)
		} else {
			p.binlogSlaveId = uint32(v)
		}
	} else {
		p.binlogSlaveId = _DEFAULT_SLAVE_ID
	}

	// ReportWarnings
	if val := query.Get("ReportWarnings"); val != "" {
		if v, err := strconv.ParseBool(val); err != nil {
			return myError(ErrInvalidProperty, "ReportWarnings", err)
		} else {
			p.reportWarnings = v
		}
	}

	// BinlogDumpNonBlock
	if val := query.Get("BinlogDumpNonBlock"); val != "" {
		if v, err := strconv.ParseBool(val); err != nil {
			return myError(ErrInvalidProperty, "BinlogDumpNonBlock", err)
		} else {
			p.binlogDumpNonBlock = v
		}
	}

	// BinlogVerifyChecksum
	if val := query.Get("BinlogVerifyChecksum"); val != "" {
		if v, err := strconv.ParseBool(val); err != nil {
			return myError(ErrInvalidProperty, "BinlogVerifyChecksum", err)
		} else {
			p.binlogVerifyChecksum = v
		}
	} else {
		p.binlogVerifyChecksum = _DEFAULT_BINLOG_VERIFY_CHECKSUM
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
