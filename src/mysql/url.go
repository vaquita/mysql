package mysql

import (
	"net/url"
	"strconv"
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

type properties struct {
	username    string
	password    string
	passwordSet bool
	host        string
	port        uint16
	schema      string

	socket string
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
	p.host, p.port = getHostPort(u.Host)
	p.schema = strings.TrimLeft(u.Path, "/")
	query := u.Query()
	p.socket = query.Get("socket")

	return nil
}

func getHostPort(hostPort string) (host string, port uint16) {
	v := strings.Split(hostPort, ":")

	switch len(v) {
	case 2:
		host = v[0]
		if p, err := strconv.ParseUint(v[1], 10, 16); err != nil {
			port = 3306
		} else {
			port = uint16(p)
		}
	case 1:
		host = v[0]
		port = 3306
	case 0:
		fallthrough
	default:
		host = "127.0.0.1"
		port = 3306
	}
	return
}
