package scioredb

import (
	"bytes"
	"testing"
)

func TestByteBufferWriteAt(t *testing.T) {
	type fields struct {
		buf []byte
	}
	type args struct {
		p   []byte
		off int64
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantN   int
		wantErr bool
	}{
		{
			name: "Negative offset",
			args: args{
				off: -1,
			},
			wantErr: true,
		},
		{
			name: "Zero offset",
			args: args{
				off: 0,
				p:   []byte{1},
			},
			wantN: 1,
		},
		{
			name: "Non-zero offset",
			args: args{
				off: 2,
				p:   []byte{0, 0},
			},
			fields: fields{
				buf: []byte{0, 0, 1, 1},
			},
			wantN:   2,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &ByteBuffer{
				buf: tt.fields.buf,
			}
			gotN, err := b.WriteAt(tt.args.p, tt.args.off)
			if (err != nil) != tt.wantErr {
				t.Errorf("WriteAt() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotN != tt.wantN {
				t.Errorf("WriteAt() gotN = %v, want %v", gotN, tt.wantN)
			}
		})
	}
}

func TestByteBufferReadAt(t *testing.T) {
	type fields struct {
		buf []byte
	}
	type args struct {
		p   []byte
		off int64
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantN   int
		wantErr bool
	}{
		{
			name: "Negative offset",
			args: args{
				off: -1,
			},
			wantErr: true,
		},
		{
			name: "Zero offset",
			args: args{
				off: 0,
				p:   []byte{0},
			},
			fields: fields{buf: []byte{1}},
			wantN:  1,
		},
		{
			name: "Non-zero offset",
			args: args{
				off: 2,
				p:   []byte{0, 0},
			},
			fields: fields{buf: []byte{1, 1, 1, 1}},
			wantN:  2,
		},
		{
			name: "EOF",
			args: args{
				off: 0,
				p:   make([]byte, 5),
			},
			fields:  fields{buf: []byte{1}},
			wantN:   1,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &ByteBuffer{
				buf: tt.fields.buf,
			}
			gotN, err := b.ReadAt(tt.args.p, tt.args.off)
			if (err != nil) != tt.wantErr {
				t.Errorf("ReadAt() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotN != tt.wantN {
				t.Errorf("ReadAt() gotN = %v, want %v", gotN, tt.wantN)
			}
		})
	}
}

func TestByteBufferNewWriteReadAtBytes(t *testing.T) {
	buf := NewByteBuffer([]byte("0"))
	_, err := buf.WriteAt([]byte("1"), 1)
	if err != nil {
		t.Errorf("WriteAt() error = %v", err)
		return
	}
	_, err = buf.WriteAt([]byte("5"), 5)
	if err != nil {
		t.Errorf("WriteAt() error = %v", err)
		return
	}
	want := []byte{'0', '1', 0, 0, 0, '5'}
	if !bytes.Equal(buf.Bytes(), want) {
		t.Errorf("ReadAt() got = %v, want %v", buf.Bytes(), want)
		return
	}
	n, err := buf.WriteAt([]byte("234"), 2)
	if err != nil {
		t.Errorf("WriteAt() error = %v", err)
		return
	}
	if n != 3 {
		t.Errorf("WriteAt() got = %v, want %v", n, 3)
		return
	}
	want = []byte("012345")
	if !bytes.Equal(buf.Bytes(), want) {
		t.Errorf("ReadAt() got = %v, want %v", buf.Bytes(), want)
		return
	}
}
