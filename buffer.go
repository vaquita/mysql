package mysql

type buffer struct {
	// the buffer
	buff []byte

	// capacity of the buffer
	cap int

	// offset from which read/write should happen
	off int

	// length of useful content in the buffer
	length int
}

func (b *buffer) New(cap int) {
	b.off, b.length = 0, 0
	b.buff = make([]byte, cap)
	b.cap = cap
}

func (b *buffer) Set(length int) {
	b.length = length
}

func (b *buffer) Len() int {
	return b.length
}

func (b *buffer) Reset(cap int) ([]byte, error) {
	b.off = 0
	b.length = 0

	if cap > b.cap {
		// simply discard the old buffer and allocate a new one
		b.buff = make([]byte, cap)
		b.cap = cap
	}

	return b.buff[0:], nil
}

func (b *buffer) Seek(off int) {
	b.off = off
}

func (b *buffer) Tell() int {
	return b.off
}

func (b *buffer) Read(length int) []byte {
	beg := b.off

	// adjust the offset
	b.off += length

	return b.buff[beg:b.off]
}

func (b *buffer) Write(p []byte) (int, error) {
	n := copy(b.buff[b.off:], p)
	b.off += n
	b.length = b.off
	return n, nil
}
