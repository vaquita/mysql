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
	// init initializes the readWriter
	init(c *Conn)

	// read reads the specified number of bytes from the network and stores
	// them into the specified buffer.
	read(b []byte, length int) (int, error)

	// write writes the contents of the specified buffer to the network.
	write([]byte) (int, error)

	// reset can be used to performs some reset operations.
	reset()
}

// defaultReadWrited implements readWriter for non-compressed network
// read/write.
type defaultReadWriter struct {
	c *Conn
}

// init is no-op.
func (rw *defaultReadWriter) init(c *Conn) {
	rw.c = c
}

// read reads the specified number of bytes from the network and stores them
// into the connection buffer.
func (rw *defaultReadWriter) read(b []byte, length int) (int, error) {
	return rw.c.netRead(b[0:length])
}

// write writes the contents of the specified buffer to network.
func (rw *defaultReadWriter) write(b []byte) (int, error) {
	return rw.c.netWrite(b)
}

// reset is no-op.
func (rw *defaultReadWriter) reset() {
}

// netRead reads len(b) number of bytes from network and stores into the
// given buffer.
func (c *Conn) netRead(b []byte) (int, error) {
	var (
		n, cur, end int
		err         error
	)

	end = len(b)

	for {
		if n, err = c.conn.Read(b[cur:end]); err != nil {

			cur += n
			return cur, myError(ErrRead, err)
		}
		cur += n
		if cur == end {
			break
		}
	}

	return end, nil
}

// netWrite writes the contents of the given buffer to the network.
func (c *Conn) netWrite(b []byte) (int, error) {
	var (
		n, cur, end int
		err         error
	)

	end = len(b)

	for {
		if n, err = c.conn.Write(b[cur:end]); err != nil {
			cur += n
			return cur, myError(ErrWrite, err)
		}
		cur += n
		if cur == end {
			break
		}
	}

	return end, nil
}
