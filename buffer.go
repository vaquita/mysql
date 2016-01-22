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
