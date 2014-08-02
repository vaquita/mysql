package mysql

import (
	"crypto/sha1"
	"encoding/binary"
)

//<!-- connection phase packets -->

// parseGreetingPacket parses handshake initialization packet received from
// the server.
func (c *Conn) parseGreetingPacket(b []byte) {
	var (
		off, n                       int
		authData                     []byte // authentiication plugin data
		authDataLength               int
		authDataOff_1, authDataOff_2 int
	)

	off++                                                 // [0a] protocol version
	c.serverVersion, n = getNullTerminatedString(b[off:]) // server version (null-terminated)
	off += n

	c.connectionId = binary.LittleEndian.Uint32(b[off : off+4]) // connection ID
	off += 4

	// auth-plugin-data-part-1 (8 bytes) : note the offset & length
	authDataOff_1 = off
	authDataLength = 8
	off += 8

	off++ // [00] filler

	// capacity flags (lower 2 bytes)
	c.serverCapabilities = uint32(binary.LittleEndian.Uint16(b[off : off+2]))
	off += 2

	if len(b) > off {
		c.serverCharset = uint8(b[off])
		off++

		c.statusFlags = binary.LittleEndian.Uint16(b[off : off+2]) // status flags
		off += 2
		// capacity flags (upper 2 bytes)
		c.serverCapabilities |= (uint32(binary.LittleEndian.Uint16(b[off:off+2])) << 16)
		off += 2

		if (c.serverCapabilities & clientPluginAuth) != 0 {
			// update the auth plugin data length
			authDataLength = int(b[off])
			off++
		} else {
			off++ // [00]
		}

		off += 10 // reserved (all [00])

		if (c.serverCapabilities & clientSecureConnection) != 0 {
			// auth-plugin-data-part-2 : note the offset & update
			// the length (max(13, authDataLength- 8)
			if (authDataLength - 8) > 13 {
				authDataLength = 13 + 8
			}
			authDataOff_2 = off
			off += (authDataLength - 8)
			authDataLength-- // ignore the 13th 0x00 byte
		}
		authData = make([]byte, authDataLength)
		copy(authData[0:8], b[authDataOff_1:authDataOff_1+8])
		if authDataLength > 8 {
			copy(authData[8:], b[authDataOff_2:authDataOff_2+(authDataLength-8)])
		}

		c.authPluginData = authData

		if (c.serverCapabilities & clientPluginAuth) != 0 {
			// auth-plugin name (null-terminated)
			c.authPluginName, n = getNullTerminatedString(b[off:])
			off += n
		}
	}
}

// createHandshakeResponsePacket generates the handshake response packet.
func (c *Conn) createHandshakeResponsePacket() []byte {
	var (
		authData []byte // auth response data
		off      int
	)

	payloadLength := (4 + 4 + 1 + 23)

	authData = c.authResponseData()
	payloadLength += c.handshakeResponse2Length(len(authData))

	b := make([]byte, 4+payloadLength)
	off += 4 // placeholder for protocol packet header

	off += c.populateHandshakeResponse1(b[off:])

	c.populateHandshakeResponse2(b[off:], authData)

	return b
}

// createSSLRequestPacket generates the SSL request packet to initiate SSL
// handshake. It is sent to the server over plain connection after which the
// communication is switched to SSL.
func (c *Conn) createSSLRequestPacket() []byte {
	payloadLength := (4 + 4 + 1 + 23)

	b := make([]byte, 4+payloadLength)

	c.populateHandshakeResponse1(b[4:])

	return b
}

// populateHandshakeResponse1 populates the specified slice with the
// information from 1st part of protocol's handshake response packet
// (before user name) and returns the final offset.
func (c *Conn) populateHandshakeResponse1(b []byte) int {
	var off int

	// client capability flags
	binary.LittleEndian.PutUint32(b[off:off+4], c.p.clientCapabilities)
	off += 4

	// max packet size
	binary.LittleEndian.PutUint32(b[off:off+4], c.p.maxPacketSize)
	off += 4

	// client character set
	b[off] = byte(c.clientCharset) // client character set
	off++

	off += 23 // reserved (all [0])

	return off
}

// populateHandshakeResponse2 populates the specified slice with the
// information from 2st part of protocol's handshake response packet
// (starting user name) and returns the final offset.
func (c *Conn) populateHandshakeResponse2(b []byte, authData []byte) int {
	var off int

	off += putNullTerminatedString(b[off:], c.p.username)

	if (c.serverCapabilities & clientPluginAuthLenencClientData) != 0 {
		off += putLenencString(b[off:], string(authData))
	} else if (c.serverCapabilities & clientSecureConnection) != 0 {
		b[off] = byte(len(authData))
		off++
		off += copy(b[off:], authData)
	} else {
		off += putNullTerminatedString(b[off:], string(authData))
	}

	if (c.p.schema != "") && ((c.serverCapabilities & clientConnectWithDb) != 0) {
		off += putNullTerminatedString(b[off:], c.p.schema)
	}

	if (c.serverCapabilities & clientPluginAuth) != 0 {
		off += putNullTerminatedString(b[off:], c.authPluginName)
	}

	if (c.serverCapabilities & clientConnectAttrs) != 0 {
		// TODO: handle connection attributes
	}
	return off
}

// handshakeResponse2Length returns the extra payload length of the handshake
// response packet starting user name.
func (c *Conn) handshakeResponse2Length(authLength int) (length int) {
	length += (len(c.p.username) + 1) // null-terminated username
	length += authLength

	if (c.serverCapabilities & clientConnectWithDb) != 0 {
		length += (len(c.p.schema) + 1) // null-terminated schema name
	}

	if (c.serverCapabilities & clientPluginAuth) != 0 {
		length += (len(c.authPluginName) + 1) // null-terminated authentication plugin name
	}

	if (c.serverCapabilities & clientConnectAttrs) != 0 {
		// TODO: handle connection attributes
	}
	return
}

// handshake performs handshake during connection establishment
func (c *Conn) handshake() (err error) {
	var (
		b      []byte
		useSSL bool
	)

	// read handshake initialization packet.
	if b, err = c.readPacket(); err != nil {
		return
	}

	c.parseGreetingPacket(b)

	// note : server capabilities can only be checked after receiving the
	// "greeting" packet
	if c.serverCapabilities&clientSSL != 0 &&
		c.p.clientCapabilities&clientSSL != 0 {
		useSSL = true
	}

	if !useSSL {
		// send plain handshake response packet
		if err = c.writePacket(c.createHandshakeResponsePacket()); err != nil {
			return
		}
	} else {
		// send SSL request packet (1st part of handshake response
		// packet)
		if err = c.writePacket(c.createSSLRequestPacket()); err != nil {
			return
		}

		// switch to tls
		if err = c.sslConnect(); err != nil {
			return
		}

		// now send the entire handshake response packet
		if err = c.writePacket(c.createHandshakeResponsePacket()); err != nil {
			return
		}
	}

	// read server response
	if b, err = c.readPacket(); err != nil {
		return
	}

	switch b[0] {
	case errPacket:
		c.parseErrPacket(b)
		return &c.e
	case okPacket:
		c.parseOkPacket(b)
		return nil
	default:
		// TODO: invalid packet
	}
	return nil
}

// authResponseData returns the authentication response data to be sent to the
// server.
func (c *Conn) authResponseData() []byte {
	return scramble41(c.p.password, c.authPluginData)
}

// scraamble41 returns a scramble buffer based on the following formula:
// SHA1(password) XOR SHA1("20-byte public seed from server" <concat> SHA1( SHA1( password)))
func scramble41(password string, seed []byte) (buf []byte) {
	if len(password) == 0 {
		return
	}

	hash := sha1.New()

	// stage 1: SHA1(password)
	hash.Write([]byte(password))
	hashStage1 := hash.Sum(nil)

	// stage 2: SHA1(SHA1(password))
	hash.Reset()
	hash.Write(hashStage1)
	hashStage2 := hash.Sum(nil)

	// SHA1("20-byte public seed from server" <concat> SHA1(SHA1(password)))
	hash.Reset()
	hash.Write(seed)
	hash.Write(hashStage2)
	buf = hash.Sum(nil)

	for i := 0; i < sha1.Size; i++ {
		buf[i] ^= hashStage1[i]
	}
	return
}
