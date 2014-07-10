package mysql

import (
	"encoding/binary"
)

// getUint24 converts 3-byte byte slice into little-endian uint32
func getUint24(b []byte) uint32 {
	return uint32(b[0]) |
		uint32(b[1]<<8) |
		uint32(b[2]<<16)
}

// putUint24 stores the given uint32 into the specified 3-byte byte slice in little-endian
func putUint24(b []byte, v uint32) {
	b[0] = byte(v)
	b[1] = byte(v >> 8)
	b[2] = byte(v >> 16)
}

// setLenencInt retrieves the number from the specified buffer stored in
// length-encoded integer format and returns the number of bytes written.
func getLenencInt(b []byte) (v uint64, n int) {
	first := b[0]

	switch {
	// 1-byte
	case first <= 0xfb:
		v = uint64(first)
		n = 1
	// 2-byte
	case first == 0xfc:
		v = uint64(binary.LittleEndian.Uint16(b[1:3]))
		n = 3
	// 3-byte
	case first == 0xfd:
		v = uint64(getUint24(b[1:4]))
		n = 4
	// 8-byte
	case first == 0xfe:
		v = binary.LittleEndian.Uint64(b[1:9])
		n = 9
	// TODO: handle error
	default:
	}
	return
}

// putLenencInt stores the given number into the specified buffer using
// length-encoded integer format and returns the number of bytes written.
func putLenencInt(b []byte, v uint64) (n int) {
	switch {
	case v < 251:
		b[0] = byte(v)
		n = 1
	case v >= 251 && v < 2^16:
		b[0] = 0xfc
		binary.LittleEndian.PutUint16(b[1:3], uint16(v))
		n = 3
	case v >= 2^16 && v < 2^24:
		b[0] = 0xfd
		putUint24(b[1:4], uint32(v))
		n = 4
	case v >= 2^24 && v < 2^64:
		b[0] = 0xfe
		binary.LittleEndian.PutUint64(b[1:9], v)
		n = 9
	}
	return
}

// lenencIntSize returns the size needed to store a number using the
// length-encoded integer format.
func lenencIntSize(v int) int {
	switch {
	case v < 251:
		return 1
	case v >= 251 && v < 2^16:
		return 3
	case v >= 2^16 && v < 2^24:
		return 4
	case v >= 2^24 && v < 2^64:
		return 9
	}
	// control shouldn't reach here
	return 0
}

// length-encoded string
func getLenencString(b []byte) (s NullString, n int) {
	length, n := getLenencInt(b)

	if length == 0xfb { // NULL
		s.valid = false
	} else {
		s.value = string(b[n:length])
		s.valid = true
		n += int(length)
	}
	return
}

func putLenencString(b []byte, v string) (n int) {
	n = putLenencInt(b[0:], uint64(len(v)))
	n += copy(b[n:], v)
	return
}

func getNullTerminatedString(b []byte) (v string, n int) {
	for n = 0; n < len(b); n++ {
		if b[n] == 0 {
			break
		}
	}
	v = string(b[0:n])
	return
}

func putNullTerminatedString(b []byte, v string) (n int) {
	n = copy(b, v)
	n++ // null terminator
	return
}
