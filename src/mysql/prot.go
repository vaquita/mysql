package mysql

import (
	"bytes"
	"encoding/binary"
)

const (
	packetHeaderSize = 4
)

const (
	comSleep = iota
	comQuit
	comInitDb
	comQuery
	comFieldList
	comCreateDb
	comDropDb
	comRefresh
	comShutdown
	comStatistics
	comProcessInfo
	comConnect
	comProcessKill
	comDebug
	comPing
	comTime
	comDelayedInsert
	comChangeUser
	comBinlogDump
	comTableDump
	comConnectOuT
	comRegisterSlave
	comStmtPrepare
	comStmtExecute
	comStmtSendLongData
	comStmtClose
	comStmtReset
	comSetOptioN
	comStmtFetch
	comDaemon
	comEnd // must always be last
)

const (
	clientLongPassword = 1 << iota
	clientFoundRows
	clientLongFlag
	clientConnectWithDb
	clientNoSchema
	clientCompress
	clientODBC
	clientLocalFiles
	clientIgnoreSpace
	clientProtocol41
	clientInteractive
	clientSSL
	clientIgnoreSIGPIPE
	clientTransactions
	clientReserved
	clientSecureConnection
	clientMultiStatements
	clientMultiResults
	clientPSMultiResults
	clientPluginAuth
	clientConnectAttrs
	clientPluginAuthLenencClientData
	clientCanHandleExpiredPasswords
	clientSessionTrack
	// ...
	clientProgress            = 1 << 29
	clientSSLVerifyServerCert = 1 << 30
	clientRememberOptions     = 1 << 31
)

//<!-- generic response packets -->

// parseOKPacket parses the OK packet received from the server.
func (c *Conn) parseOKPacket(b *bytes.Buffer) {
	b.Next(1) // [00] the OK header
	c.affectedRows = getLenencInteger(b)
	c.lastInsertId = getLenencInteger(b)

	c.statusFlags = binary.LittleEndian.Uint16(b.Next(2))
	c.warnings = binary.LittleEndian.Uint16(b.Next(2))
	// TODO : read rest of fields
}

// parseERRPacket parses the ERR packet received from the server.
func (c *Conn) parseERRPacket(b *bytes.Buffer) {
	b.Next(1) // [ff] the ERR header
	c.errorCode = binary.LittleEndian.Uint16(b.Next(2))
	b.Next(1) // '#' the sql-state marker
	c.sqlState = string(b.Next(5))
	c.errorMessage = string(b.Next(b.Len()))
}

// parseEOFPacket parses the EOF packet received from the server.
func (c *Conn) parseEOFPacket(b *bytes.Buffer) {
	b.Next(1) // [fe] the EOF header
	c.warnings = binary.LittleEndian.Uint16(b.Next(2))
	c.statusFlags = binary.LittleEndian.Uint16(b.Next(2))
}

//<!-- connection phase packets -->

// parseHandshakePacket
func (c *Conn) parseHandshakePacket(b *bytes.Buffer) {
	var authPluginDataBuf bytes.Buffer

	b.Next(1)                                              // [0a] protocol version
	c.serverVersion, _ = b.ReadString(0)                   // server version (null-terminated)
	c.connectionId = binary.LittleEndian.Uint32(b.Next(4)) // connection ID

	// auth-plugin-data-part-1 (8 bytes)
	authPluginDataBuf.WriteString(string(b.Next(8)))

	b.Next(1) // [00] filler
	// capacity flags (lower 2 bytes)
	c.serverCapabilityFlags = uint32(binary.LittleEndian.Uint16(b.Next(2)))

	if b.Len() > 0 {
		c.serverCharacterSet = uint8(b.Next(1)[0])
		c.statusFlags = binary.LittleEndian.Uint16(b.Next(2)) // status flags
		// capacity flags (upper 2 bytes)
		c.serverCapabilityFlags = uint32(binary.LittleEndian.Uint16(b.Next(2)) << 2)

		if (c.serverCapabilityFlags & clientPluginAuth) != 0 {
			c.authPluginDataLength = uint8(b.Next(1)[0])
		} else {
			b.Next(1) // [00]
		}

		b.Next(10) // reserved (all [00])

		if (c.serverCapabilityFlags & clientSecureConnection) != 0 {
			// auth-plugin-data-part-1 (8 bytes)
			// max(13, c.authPluginDataLength - 8)
			authPluginDataBuf.WriteString(string(b.Next(13)))
		}
		c.authPluginData = authPluginDataBuf.String()

		if (c.serverCapabilityFlags & clientPluginAuth) != 0 {
			// auth-plugin name (null-terminated)
			c.authPluginName, _ = b.ReadString(0)
		}
	}
}

// createHandshakeResponsePacket
func (c *Conn) createHandshakeResponsePacket() *bytes.Buffer {
	payloadLength := c.handshakeResponsePacketLength()

	b := bytes.NewBuffer(make([]byte, packetHeaderSize+payloadLength))
	b.Next(4)

	// client capability flags
	binary.LittleEndian.PutUint32(b.Next(4), c.clientCapabilityFlags)
	// max packaet size
	binary.LittleEndian.PutUint32(b.Next(4), c.maxPacketSize)
	b.WriteByte(c.clientCharacterSet) // client character set
	b.Next(23)                        // reserved (all [0])

	putNullTerminatedString(b, c.user)

	if (c.clientCapabilityFlags & clientPluginAuthLenencClientData) != 0 {
		putLenencString(b, c.authResponseData)
	} else if (c.clientCapabilityFlags & clientSecureConnection) != 0 {
		b.WriteByte(byte(len(c.authResponseData)))
		b.WriteString(c.authResponseData)
	} else {
		putNullTerminatedString(b, c.authResponseData)
	}

	if (c.clientCapabilityFlags & clientConnectWithDb) != 0 {
		putNullTerminatedString(b, c.schema)
	}

	if (c.clientCapabilityFlags & clientConnectAttrs) != 0 {
		// TODO: handle connection attributes
	}
	return b
}

func (c *Conn) handshakeResponsePacketLength() int {
	return 0
}

//<!-- command phase packets -->
