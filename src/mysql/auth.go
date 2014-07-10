package mysql

import (
	"crypto/sha1"
)

// handshake performs handshake during connection establishment
func (c *Conn) handshake() (err error) {
	var b []byte

	// read handshake initialization packet.
	if b, err = c.readPacket(); err != nil {
		return
	}

	c.parseGreetingPacket(b)

	// send handshake response packet
	if err = c.writePacket(c.createHandshakeResponsePacket()); err != nil {
		return
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

// formula :
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
