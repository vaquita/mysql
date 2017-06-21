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
	"database/sql/driver"
	"encoding/binary"
	"hash/crc32"
)

const _BINLOG_CHECKSUM_LENGTH = 4

const (
	BINLOG_CHECKSUM_ALG_OFF = iota
	BINLOG_CHECKSUM_ALG_CRC32
	BINLOG_CHECKSUM_ALG_END
	BINLOG_CHECKSUM_ALG_UNDEF = 255
)

type checksumVerifier interface {
	algorithm() uint8
	test(ev []byte) bool
}

type checksumOff struct{}

func (c *checksumOff) algorithm() uint8 {
	return BINLOG_CHECKSUM_ALG_OFF
}

func (c *checksumOff) test(ev []byte) bool {
	return true
}

type checksumCRC32IEEE struct{}

func (c *checksumCRC32IEEE) algorithm() uint8 {
	return BINLOG_CHECKSUM_ALG_CRC32
}

// test verifies the checksum for an event and returns true if it
// passes, false otherwise. note : the checksum computed on the master is
// stored in the last _BINLOG_CHECKSUM_LENGTH bytes of the event
func (c *checksumCRC32IEEE) test(ev []byte) bool {
	var (
		checksumReceived uint32
		checksumComputed uint32
		flags            uint16
		flags_orig       uint16
		changed          bool
	)

	beg := len(ev) - _BINLOG_CHECKSUM_LENGTH
	end := beg + _BINLOG_CHECKSUM_LENGTH

	checksumReceived = binary.LittleEndian.Uint32(ev[beg:end])

	if ev[4] == FORMAT_DESCRIPTION_EVENT {
		/*
		   FD event is checksummed without the
		   _LOG_EVENT_BINLOG_IN_USE_F flag
		*/
		flags = binary.LittleEndian.Uint16(ev[_FLAGS_OFFSET:])
		if (flags & _LOG_EVENT_BINLOG_IN_USE_F) != 0 {
			flags_orig = flags
			changed = true
			flags &= ^uint16(_LOG_EVENT_BINLOG_IN_USE_F)
			binary.LittleEndian.PutUint16(ev[_FLAGS_OFFSET:], flags)
		}

		checksumComputed = crc32.ChecksumIEEE(ev[0:beg])

		// restore the flags
		if changed {
			binary.LittleEndian.PutUint16(ev[_FLAGS_OFFSET:], flags_orig)
		}
	} else {

		checksumComputed = crc32.ChecksumIEEE(ev[0:beg])
	}

	if checksumReceived == checksumComputed {
		return true
	}
	return false
}

// notifyChecksumAwareness notifies master of its checksum capabilities.
func notifyChecksumAwareness(c *Conn) error {
	_, err := c.handleExec("SET @master_binlog_checksum= @@global.binlog_checksum", nil)
	return err
}

// fetchBinlogChecksum get checksum algorithm.
func fetchBinlogChecksum(c *Conn) (checksumVerifier, error) {
	var checksum checksumVerifier
	checksum = new(checksumOff)
	rows, err := c.Query("show global variables like 'binlog_checksum'", nil)
	if err != nil {
		return checksum, err
	}
	defer rows.Close()
	var dest = make([]driver.Value, len(rows.Columns()))
	err = rows.Next(dest)
	if err != nil {
		return checksum, err
	}
	switch dest[1].(string) {
	case "CRC32":
		checksum = new(checksumCRC32IEEE)
	default:

	}

	return checksum, err

}

// updateChecksumVerifier updates the current checksum verifier
func updateChecksumVerifier(b *Binlog) {
	// return if checksum algorithm has not changed
	if b.checksum.algorithm() == b.desc.checksumAlg {
		return
	}

	switch b.desc.checksumAlg {
	case BINLOG_CHECKSUM_ALG_OFF:
		b.checksum = new(checksumOff)
	case BINLOG_CHECKSUM_ALG_CRC32:
		b.checksum = new(checksumCRC32IEEE)
	default:
		// TODO: verify?
		b.checksum = new(checksumOff)
	}

	return
}
