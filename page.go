package scioredb

import (
	"encoding/binary"
	"time"
)

type Page struct {
	buf *ByteBuffer
}

func NewPage(b []byte) *Page {
	return &Page{buf: NewByteBuffer(b)}
}

func (p *Page) InitBuffer(b []byte) {
	p.buf = NewByteBuffer(b)
}

func (p *Page) writeAt(b []byte, off int64) (n int, err error) {
	n, err = p.WriteInt(len(b), off)
	if err != nil {
		return
	}
	var n2 int
	n2, err = p.buf.WriteAt(b, off+int64(n))
	n += n2
	return
}

func (p *Page) readAt(off int64) (b []byte, err error) {
	var l int
	l, err = p.ReadInt(off)
	if err != nil {
		return
	}
	b = make([]byte, l)
	_, err = p.buf.ReadAt(b, off+8)
	return
}

func (p *Page) WriteBool(value bool, off int64) (n int, err error) {
	b := make([]byte, 1)
	if value {
		b[0] = 1
	}
	return p.buf.WriteAt(b, off)
}

func (p *Page) ReadBool(off int64) (v bool, err error) {
	b := make([]byte, 1)
	_, err = p.buf.ReadAt(b, off)
	if err != nil {
		return
	}
	v = b[0] == 1
	return
}

func (p *Page) WriteInt(value int, off int64) (n int, err error) {
	b := make([]byte, 8)
	binary.PutVarint(b, int64(value))
	return p.buf.WriteAt(b, off)
}

func (p *Page) ReadInt(off int64) (v int, err error) {
	b := make([]byte, 8)
	_, err = p.buf.ReadAt(b, off)
	if err != nil {
		return
	}
	i, _ := binary.Varint(b)
	return int(i), nil
}

func (p *Page) WriteString(value string, off int64) (n int, err error) {
	n, err = p.writeAt([]byte(value), off)
	return
}

func (p *Page) ReadString(off int64) (string, error) {
	b, err := p.readAt(off)
	return string(b), err
}

func (p *Page) WriteTime(value time.Time, off int64) (n int, err error) {
	var b []byte
	b, err = value.MarshalBinary()
	if err != nil {
		return 0, err
	}
	n, err = p.writeAt(b, off)
	return
}

func (p *Page) ReadTime(off int64) (v time.Time, err error) {
	var b []byte
	b, err = p.readAt(off)
	if err != nil {
		return
	}
	err = v.UnmarshalBinary(b)
	return
}

func (p *Page) Bytes() []byte {
	return p.buf.Bytes()
}
