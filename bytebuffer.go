package scioredb

import (
	"errors"
	"fmt"
	"io"
)

var ErrNegativeOffset = errors.New("negative offset")

type ByteBuffer struct {
	buf []byte
}

func NewByteBuffer(buf []byte) *ByteBuffer {
	return &ByteBuffer{buf: buf}
}

func (b *ByteBuffer) WriteAt(p []byte, off int64) (n int, err error) {
	if off < 0 {
		return 0, fmt.Errorf("ByteBuffer.WriteAt: %w", ErrNegativeOffset)
	}
	minLen := off + int64(len(p))
	if minLen > int64(len(b.buf)) {
		buf := make([]byte, minLen)
		copy(buf, b.buf)
		b.buf = buf
	}
	n = copy(b.buf[off:], p)
	return
}

func (b *ByteBuffer) ReadAt(p []byte, off int64) (n int, err error) {
	if off < 0 {
		return 0, fmt.Errorf("ByteBuffer.ReadAt: %w", ErrNegativeOffset)
	}
	if off >= int64(len(b.buf)) {
		return 0, io.EOF
	}
	n = copy(p, b.buf[off:])
	if n < len(p) {
		err = io.EOF
	}
	return n, err
}

func (b *ByteBuffer) Len() int {
	return len(b.buf)
}

func (b *ByteBuffer) Bytes() []byte {
	return b.buf
}
