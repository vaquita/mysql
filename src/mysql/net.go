package mysql

import (
	"net"
)

// dial opens a connection with the server; prefer socket if specified.
func dial(address, socket string) (c net.Conn, err error) {
	var addr, network string

	if socket != "" {
		network, addr = "socket", socket
	} else {
		network, addr = "tcp", address
	}
	return net.Dial(network, addr)
}

type readWriter interface {
	read(c net.Conn, b []byte) (n int, err error)
	write(c net.Conn, b []byte) (n int, err error)
}

type defaultReadWriter struct {
}

func (rw *defaultReadWriter) read(c net.Conn, b []byte) (n int, err error) {
	return c.Read(b)
}

func (rw *defaultReadWriter) write(c net.Conn, b []byte) (n int, err error) {
	return c.Write(b)
}
