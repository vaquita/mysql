package mysql

import (
	"net"
)

type _net struct {
	conn net.Conn
}

func (n *_net) read(b []byte) (int, error) {
	return n.conn.Read(b)
}

func (n *_net) write(b []byte) (int, error) {
	return n.conn.Write(b)
}
