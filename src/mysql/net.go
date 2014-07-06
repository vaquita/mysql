package mysql

import (
	"net"
)

type _net struct {
	c net.Conn
}

// dial opens a connection with the server; prefer socket if specified.
func (n *_net) dial(address, socket string) error {
	var err error

	if socket != "" {
		n.c, err = net.Dial("unix", socket)
	} else {
		n.c, err = net.Dial("tcp", address)
	}
	return err
}

func (n *_net) read(b []byte) (int, error) {
	return n.c.Read(b)
}

func (n *_net) write(b []byte) (int, error) {
	return n.c.Write(b)
}
