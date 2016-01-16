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
)

type compressRW struct {
	c     *Conn
	cbuff buffer // buffer to hold compressed packet
	ubuff buffer // buffer to hold uncompressed packet(s)
	seqno uint8  // packet sequence number
}

func (rw *compressRW) init(c *Conn) {
	rw.c = c
	rw.cbuff.New(_INITIAL_PACKET_BUFFER_SIZE)
	rw.ubuff.New(_INITIAL_PACKET_BUFFER_SIZE)
}

// read reads a compressed protocol packet from network (when required),
// uncompresses and caches it so that the uncompressed content of requested
// size can be copied to given buffer.
func (rw *compressRW) read(b []byte, length int) (int, error) {
	var (
		n, unread int
		err       error
	)

	// unread bytes in the buffer
	unread = rw.ubuff.Len() - rw.ubuff.Tell()

	// read a compressed packet if the local buffer (ubuff) is either
	// empty, fully read or does not have enough requested unread bytes
	if length > unread {
		if err = rw.readCompressedPacket(unread); err != nil {
			return 0, err
		}
	}

	// fill the supplied buffer with contents from locally cached
	// uncompressed packet buffer of specified length.
	n = copy(b, rw.ubuff.Read(length))

	return n, nil
}

// readCompressedPacket reads a compressed protocol packet from network into
// the local compressed packet buffer (cbuff), uncompresses it and caches it
// into the local cache for uncompressed packets (ubuff)
func (rw *compressRW) readCompressedPacket(unread int) error {
	var (
		payloadLength, origPayloadLength int
		cbuff, old                       []byte
		err                              error
	)

	// save unread bytes from the buffer
	if unread > 0 {
		old = make([]byte, unread)
		copy(old, rw.ubuff.Read(unread))
	}

	// read the compressed packet header into the compressed packet
	// buffer (cbuff) and parse it
	if cbuff, err = rw.cbuff.Reset(7); err != nil {
		return err
	}

	if _, err = rw.c.netRead(cbuff[0:7]); err != nil {
		return myError(ErrRead, err)
	}

	// packet payload length
	payloadLength = int(getUint24(cbuff[0:3]))

	// check for out-of-order packets
	if rw.seqno != cbuff[3] {
		return myError(ErrNetPacketsOutOfOrder)
	}

	// length of payload before compression
	origPayloadLength = int(getUint24(cbuff[4:7]))

	// increment the packet sequence number
	rw.seqno++

	// error out if the packet is too big
	if payloadLength+7 > int(rw.c.p.maxPacketSize) {
		return myError(ErrNetPacketTooLarge)
	}

	// read compressed protocol packet payload from the network into
	// the compressed packet buffer (note: the header gets overwritten)
	if cbuff, err = rw.cbuff.Reset(payloadLength); err != nil {
		return err
	}
	if _, err = rw.c.netRead(cbuff[0:payloadLength]); err != nil {
		return myError(ErrRead, err)
	}

	// at this point we have the packet payload stored into the compressed
	// packet buffer (cbuff), uncompress (if needed) and store it into the
	// uncompressed packet buffer (ubuff).

	if origPayloadLength != 0 { // its a compressed payload
		var (
			src io.ReadCloser
		)

		if _, err = rw.ubuff.Reset(origPayloadLength + unread); err != nil {
			return err
		}

		// reload the unread bytes from old buffer
		if unread > 0 {
			rw.ubuff.Write(old)
		}

		if src, err = zlib.NewReader(bytes.NewReader(cbuff[0:payloadLength])); err != nil {
			return myError(ErrCompression, err)
		} else if _, err = io.Copy(&rw.ubuff, src); err != nil {
			return myError(ErrCompression, err)
		}
	} else { // its an uncompressed payload, simply copy it
		if _, err = rw.ubuff.Reset(payloadLength + unread); err != nil {
			return err
		}

		// reload the unread bytes from old buffer
		if unread > 0 {
			rw.ubuff.Write(old)
		}

		rw.ubuff.Write(cbuff[0:payloadLength])
	}

	// reset for reading
	rw.ubuff.Seek(0)

	return nil
}

// write creates a compressed protocol packet with the specified payload and
// writes it to the network.
func (rw *compressRW) write(b []byte) (int, error) {
	var (
		cbuff []byte
		n     int
		err   error
	)

	// TODO: add a property for compression threshold
	if len(b) > 50 { // compress the payload
		if cbuff, err = rw.createCompPacket(b); err != nil {
			return 0, err
		}
	} else { // no need to compress the payload
		if cbuff, err = rw.createRegPacket(b); err != nil {
			return 0, err
		}
	}

	// increment the packet sequence number
	rw.seqno++

	if n, err = rw.c.netWrite(cbuff); err != nil {
		return n, myError(ErrWrite, err)
	}

	return n, nil
}

// createCompPacket generates a compressed protocol packet after
// compressing the given payload.
func (rw *compressRW) createCompPacket(b []byte) ([]byte, error) {
	var (
		w             *zlib.Writer
		z             bytes.Buffer
		cbuff         []byte
		err           error
		payloadLength int
		off           int
	)

	// TODO: add a property for compression level
	if w, err = zlib.NewWriterLevel(&z, zlib.DefaultCompression); err != nil {
		goto E
	}

	if _, err = w.Write(b); err != nil {
		goto E
	}

	if err = w.Close(); err != nil {
		goto E
	}

	payloadLength = z.Len()

	if cbuff, err = rw.cbuff.Reset(7 + payloadLength); err != nil {
		return nil, err
	}

	// compressed header
	// - size of compressed payload
	putUint24(cbuff[0:3], uint32(payloadLength))
	// - packet sequence number
	cbuff[3] = rw.seqno
	// - size of payload before it was compressed
	putUint24(cbuff[4:7], uint32(len(b)))
	off += 7

	// copy the compressed payload
	off += copy(cbuff[7:], z.Bytes())

	return cbuff[0:off], nil

E:
	return nil, myError(ErrCompression, err)
}

// createRegPacket generates a non-compressed protocol packet from the specified
// payload.
func (rw *compressRW) createRegPacket(b []byte) ([]byte, error) {
	var (
		cbuff              []byte
		off, payloadLength int
		err                error
	)

	payloadLength = len(b)

	if cbuff, err = rw.cbuff.Reset(7 + payloadLength); err != nil {
		return nil, err
	}

	// compressed header
	// - size of compressed payload
	putUint24(cbuff[0:3], uint32(payloadLength))

	// - packet sequence number
	cbuff[3] = rw.seqno

	// - = 0, because the payload is not being compressed
	putUint24(cbuff[4:7], uint32(0))
	off += 7

	// store the payload (as is)
	off += copy(cbuff[7:], b)

	return cbuff[0:off], nil
}

// reset resets the packet sequence number.
func (rw *compressRW) reset() {
	rw.seqno = 0
}
