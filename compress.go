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
	"bytes"
	"compress/zlib"
	"io"
	"net"
)

type compressRW struct {
	readBuffer bytes.Buffer
	filled     bool
	seqno      uint8 // packet sequence number
}

// read reads a compressed protocol packet from network (when required),
// uncompresses and caches it so that the uncompressed content of requested
// size can be sent to the caller.
func (rw *compressRW) read(c net.Conn, b []byte) (n int, err error) {

	// read a compressed packet if the buffer is empty.
	if !rw.filled {
		if err = rw.readCompressedPacket(c); err != nil {
			return
		}
	}

	// fill the specified buffer
	copy(b, rw.readBuffer.Next(len(b)))

	if rw.readBuffer.Len() == 0 {
		rw.filled = false
		rw.readBuffer.Reset()
	}

	return
}

// readCompressedPacket reads a single compressed protocol packet.
func (rw *compressRW) readCompressedPacket(c net.Conn) error {
	var err error

	// read compressed packet header and parse it
	header := make([]byte, 7)
	if _, err = c.Read(header); err != nil {
		return myError(ErrRead, err)
	}

	payloadLength := getUint24(header[0:3])    // packet payload length
	origPayloadLength := getUint24(header[4:]) // length of payload before compression

	// increment the packet sequence number
	rw.seqno++

	// read compressed protocol packet payload from the network
	payload := make([]byte, payloadLength)
	if _, err = c.Read(payload); err != nil {
		return myError(ErrRead, err)
	}

	if origPayloadLength != 0 { // its a compressed payload
		var r io.ReadCloser

		if r, err = zlib.NewReader(bytes.NewBuffer(payload)); err != nil {
			return myError(ErrCompression, err)
		} else {
			io.Copy(&rw.readBuffer, r)
		}

	} else { // its an uncompressed payload
		if _, err = rw.readBuffer.Write(payload); err != nil {
			return myError(ErrCompression, err)
		}
	}

	rw.filled = true
	return nil
}

// write creates a compressed protocol packet with the specified payload and
// writes it to the network.
func (rw *compressRW) write(c net.Conn, b []byte) (n int, err error) {
	var packet []byte

	// TODO: add a property for compression threshold
	if len(b) > 50 { // compress the payload
		if packet, err = rw.createCompressedPacket(b); err != nil {
			return
		}
	} else { // no need to compress the payload
		packet = rw.createPacket(b)
	}

	// increment the packet sequence number
	rw.seqno++

	return c.Write(packet)
}

// createCompressedPacket generates a compressed protocol packet after
// compressing the specified payload.
func (rw *compressRW) createCompressedPacket(payload []byte) ([]byte, error) {
	var (
		w             *zlib.Writer
		z             bytes.Buffer
		packet        []byte
		err           error
		payloadLength int
	)

	// TODO: add a property for compression level
	if w, err = zlib.NewWriterLevel(&z, zlib.DefaultCompression); err != nil {
		goto E
	}

	if _, err = w.Write(payload); err != nil {
		goto E
	}

	if err = w.Close(); err != nil {
		goto E
	}

	payloadLength = z.Len()

	// allocate buffer for the compressed packet
	// header (7 bytes) + payload
	packet = make([]byte, 7+payloadLength)

	// compressed header
	// - size of compressed payload
	putUint24(packet[0:3], uint32(payloadLength))
	// - packet sequence number
	packet[3] = rw.seqno
	// - size of payload before it was compressed
	putUint24(packet[4:7], uint32(len(payload)))

	// copy the compressed payload
	copy(packet[7:], z.Bytes())

	return packet, nil

E:
	return nil, myError(ErrCompression, err)

}

// createPacket generates a non-compressed protocol packet from the specified
// payload.
func (rw *compressRW) createPacket(payload []byte) (packet []byte) {
	payloadLength := len(payload)
	// allocate buffer for the compressed packet
	// header (7 bytes) + payload
	packet = make([]byte, 7+payloadLength)

	// compressed header
	// - size of compressed payload
	putUint24(packet[0:3], uint32(payloadLength))
	// - packet sequence number
	packet[3] = rw.seqno
	// - = 0, because the payload is not being compressed
	putUint24(packet[4:7], uint32(0))

	// store the payload (as is)
	copy(packet[7:], payload)
	return
}

// reset resets the packet sequence number.
func (rw *compressRW) reset() {
	rw.seqno = 0
}
