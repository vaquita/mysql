package mysql

import (
	"net/url"
	"strings"
)

/*
  Reference :
  http://docs.oracle.com/javase/tutorial/jdbc/basics/connecting.html

  mysql://[host][,failoverhost...]
    [:port]/[database]
    [?propertyName1][=propertyValue1]
    [&propertyName2][=propertyValue2]...

  * host:port -  It is the host name and port number of the machine running
                 MySQL/MariaDB server. (default : 127.0.0.1:3306)
  * database  -  Name of the database to connect to.
  * propertyName=propertyValue
              - It represents an optional, ampersand-separated list of
                properties.
  eg. "mysql://root:pass@localhost:3306/test?socket=/tmp/mysql.sock"
*/

const (
	defaultHost          = "127.0.0.1"
	defaultPort          = "3306"
	defaultMaxPacketSize = 1024 * 1024
)

type properties struct {
	username    string
	password    string
	passwordSet bool
	address     string // host:port
	socket      string
	schema      string
}

func (p *properties) parseUrl(dsn string) error {
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
	query := u.Query()
	p.socket = query.Get("socket")

	return nil
}

// address returns the address in 'host:port' format. default ip (127.0.0.1) and
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
