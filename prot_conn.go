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

type Conn struct {
	// connection properties
	p properties

	conn  net.Conn
	rw    readWriter
	seqno uint8 // packet sequence number

	// OK packet
	affectedRows uint64
	lastInsertId uint64
	statusFlags  uint16
	warnings     uint16

	// ERR packet
	e Error

	// handshake initialization packet (from server)
	serverVersion      string
	connectionId       uint32
	serverCapabilities uint32
	serverCharset      uint8
	authPluginData     []byte
	authPluginName     string

	// handshake response packet (from client)
	clientCharset uint8
}

func open(p properties) (*Conn, error) {
	var err error

	c := &Conn{}
	c.rw = &defaultReadWriter{}
	c.p = p

	// open a connection with the server
	if c.conn, err = dial(p.address, p.socket); err != nil {
		return nil, err
	}

	// perform handshake
	if err = c.handshake(); err != nil {
		return nil, err
	}

	return c, nil
}

// readPacket reads the next protocol packet from the network and returns the
// payload after increment the packet sequence number.
func (c *Conn) readPacket() ([]byte, error) {
	var err error

	// first read the packet header
	header := make([]byte, 4)
	if _, err = c.rw.read(c.conn, header); err != nil {
		return nil, err
	}

	// payload length
	payloadLength := getUint24(header[0:3])

	// increment the packet sequence number
	c.seqno++

	// finally, read the payload
	payload := make([]byte, payloadLength)
	if _, err = c.rw.read(c.conn, payload); err != nil {
		return nil, err
	}
	return payload, nil
}

// writePacket accepts the protocol packet to be written, populates the header
// and writes it to the network.
func (c *Conn) writePacket(b []byte) error {
	var err error

	// populate the packet header
	putUint24(b[0:3], uint32(len(b)-4)) // payload length
	b[3] = c.seqno                      // packet sequence number

	// write it to the connection
	if _, err = c.rw.write(c.conn, b); err != nil {
		return err
	}

	// finally, increment the packet sequence number
	c.seqno++

	return nil
}

// resetSeqno resets the packet sequence number.
func (c *Conn) resetSeqno() {
	c.seqno = 0
	c.rw.reset()
}
