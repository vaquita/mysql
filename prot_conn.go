i/*
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
