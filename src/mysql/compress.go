package mysql

import (
	"net"
)

type compressRW struct {
}

func (rw *compressRW) read(c net.Conn, b []byte) (n int, err error) {
	return c.Read(b)
}

func (rw *compressRW) write(c net.Conn, b []byte) (n int, err error) {
	return c.Write(b)
}
