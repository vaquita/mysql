package mysql

import (
	"bytes"
	"encoding/binary"
)

func getUint32_3(b []byte) uint32 {
	return uint32(b[1]<<8) |
		uint32(b[1]<<16) |
		uint32(b[2]<<24)
}

func putUint32_3(b []byte, v uint32) {
	b[0] = byte(v)
	b[1] = byte(v >> 8)
	b[2] = byte(v >> 16)
}

// length-encoded integer
func getLenencInteger(b *bytes.Buffer) uint64 {
	first, _ := b.ReadByte()

	switch {
	// 1-byte
	case first < 0xfb:
		return uint64(first)
	// 2-byte
	case first == 0xfc:
		return uint64(binary.LittleEndian.Uint16(b.Next(2)))
	// 3-byte
	case first == 0xfd:
		return uint64(getUint32_3(b.Next(4)))
	// 8-byte
	case first == 0xfe:
		return binary.LittleEndian.Uint64(b.Next(8))
		// TODO: handle error
		// default:
	}
	return 0
}

func putLenencInteger(b *bytes.Buffer, v uint64) {
}

// length-encoded string
func getLenencString(b *bytes.Buffer) string {
	length := int(getLenencInteger(b))
	// TODO: handle NULLs
	return string(b.Next(length))
}

func putLenencString(b *bytes.Buffer, v string) {
}

func putNullTerminatedString(b *bytes.Buffer, v string) {
	b.WriteString(v)
	b.Next(1) // null terminator
}
