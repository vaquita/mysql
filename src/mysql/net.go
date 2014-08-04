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

// readWriter is a generic interface to read/write protocol packets to/from
// the network.
type readWriter interface {
	// read reads a protocol packet from the network and stores it into the
	// specified buffer.
	read(c net.Conn, b []byte) (n int, err error)

	// write writes the protocol packet (content of the specified buffer) to
	// the network.
	write(c net.Conn, b []byte) (n int, err error)

	// reset can be used to performs some reset operations.
	reset()
}

// defaultReadWrited implements readWriter for non-compressed network
// read/write.
type defaultReadWriter struct {
}

// read reads a protocol packet from the network and stores it into the
// specified buffer.
func (rw *defaultReadWriter) read(c net.Conn, b []byte) (n int, err error) {
	return c.Read(b)
}

// write writes the protocol packet (content of the specified buffer) to the
// network.
func (rw *defaultReadWriter) write(c net.Conn, b []byte) (n int, err error) {
	return c.Write(b)
}

// reset is no-op.
func (rw *defaultReadWriter) reset() {
}
