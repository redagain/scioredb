package scioredb

import (
	"errors"
	"io"
)

var errNegativeOffset = errors.New("negative offset")

type ByteBuffer struct {
	b []byte
}

func NewByteBuffer(b []byte) *ByteBuffer {
	return &ByteBuffer{b: b}
}

func (buf *ByteBuffer) WriteAt(p []byte, off int64) (n int, err error) {
	if off < 0 {
		err = errNegativeOffset
		return
	}
	if len(p) == 0 {
		return
	}
	prevLen := len(buf.b)
	diff := int(off) - prevLen
	if diff > 0 {
		buf.b = append(buf.b, make([]byte, diff)...)
	}
	buf.b = append(buf.b[:off], p...)
	if len(buf.b) < prevLen {
		buf.b = buf.b[:prevLen]
	}
	n = len(p)
	return
}

func (buf *ByteBuffer) ReadAt(p []byte, off int64) (n int, err error) {
	if off < 0 {
		err = errNegativeOffset
		return
	}
	if len(p) == 0 {
		return
	}
	if off >= int64(len(buf.b)) {
		return 0, io.EOF
	}
	n = copy(p, buf.b[off:])
	if n < len(p) {
		err = io.EOF
	}
	return
}

func (buf *ByteBuffer) Len() int {
	return len(buf.b)
}

func (buf *ByteBuffer) Bytes() []byte {
	return buf.b
}
