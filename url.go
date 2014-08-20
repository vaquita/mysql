package mysql

import (
	"net/url"
	"strconv"
	"strings"
)

// default properties
const (
	defaultHost          = "127.0.0.1"
	defaultPort          = "3306"
	defaultMaxPacketSize = 16 * 1024 * 1024 // 16MB
	defaultCapabilities  = (clientLongPassword |
		clientLongFlag |
		clientTransactions |
		clientProtocol41 |
		clientSecureConnection |
		clientMultiResults |
		clientPluginAuth)
)

type properties struct {
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
}

func (p *properties) parseUrl(dsn string) error {
	// initialize default properties
	p.clientCapabilities = defaultCapabilities

	u, err := url.Parse(dsn)
	if err != nil {
		return err
	}

	if u.User != nil {
		p.username = u.User.Username()
		p.password, p.passwordSet = u.User.Password()
	}
	p.address = u.Host
	p.address = parseHost(u.Host)

	p.schema = strings.TrimLeft(u.Path, "/")
	if p.schema != "" {
		p.clientCapabilities |= clientConnectWithDb
	}

	query := u.Query()

	// Socket
	p.socket = query.Get("Socket")

	// LocalInfile
	if val := query.Get("LocalInfile"); val != "" {
		if v, err := strconv.ParseBool(val); err != nil {
			return err
		} else if v {
			p.clientCapabilities |= clientLocalFiles
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
		p.maxPacketSize = defaultMaxPacketSize
	}

	// SSLCA
	if val := query.Get("SSLCA"); val != "" {
		p.sslCA = val
		p.clientCapabilities |= clientSSL
	}

	// SSLCert
	if val := query.Get("SSLCert"); val != "" {
		p.sslCert = val
		p.clientCapabilities |= clientSSL
	}

	// SSLKey
	if val := query.Get("SSLKey"); val != "" {
		p.sslKey = val
		p.clientCapabilities |= clientSSL
	}

	// Compress
	if val := query.Get("Compress"); val != "" {
		if v, err := strconv.ParseBool(val); err != nil {
			return err
		} else if v {
			p.clientCapabilities |= clientCompress
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
			host = defaultHost
			defaultAssigned = true
		}

		if port == "" {
			port = defaultPort
			defaultAssigned = true
		}

		if defaultAssigned == false {
			return addr // addr is already in required format
		}
		break

	case 1:
		host = v[0]
		if host == "" {
			host = defaultHost
		}
		port = defaultPort
	case 0:
		fallthrough
	default:
		host = defaultHost
		port = defaultPort
		break
	}
	return strings.Join([]string{host, port}, ":")
}
