/*
  Copyright (C) 2015 Nirbhay Choubey

  This library is free software; you can redistribute it and/or
  modify it under the terms of the GNU Lesser General Public
  License as published by the Free Software Foundation; either
  version 2.1 of the License, or (at your option) any later version.

  This library is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
  Lesser General Public License for more details.

  You should have received a copy of the GNU Lesser General Public
  License along with this library; if not, write to the Free Software
  Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301
  USA
*/

package mysql

import (
	"net"
)

// dial opens a connection with the server; prefer socket if specified.
func dial(address, socket string) (net.Conn, error) {
	var (
		c       net.Conn
		addr    string
		network string
		err     error
	)

	if socket != "" {
		network, addr = "socket", socket
	} else {
		network, addr = "tcp", address
	}

	if c, err = net.Dial(network, addr); err != nil {
		return nil, myError(ErrConnection, err)
	}
	return c, nil

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
func (rw *defaultReadWriter) read(c net.Conn, b []byte) (int, error) {
	var (
		n   int
		err error
	)

	if n, err = c.Read(b); err != nil {
		return n, myError(ErrRead, err)
	}

	return n, nil
}

// write writes the protocol packet (content of the specified buffer) to the
// network.
func (rw *defaultReadWriter) write(c net.Conn, b []byte) (int, error) {
	var (
		n   int
		err error
	)

	if n, err = c.Write(b); err != nil {
		return n, myError(ErrWrite, err)
	}

	return n, nil
}

// reset is no-op.
func (rw *defaultReadWriter) reset() {
}
