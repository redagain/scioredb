package scioredb

import (
	"reflect"
	"testing"
)

func TestNewByteBuffer(t *testing.T) {
	type args struct {
		b []byte
	}
	tests := []struct {
		name string
		args args
		want *ByteBuffer
	}{
		{
			name: "nil",
			want: &ByteBuffer{},
		},
		{
			name: "empty",
			args: args{
				b: []byte{},
			},
			want: &ByteBuffer{
				b: []byte{},
			},
		},
		{
			name: "0123456789",
			args: args{
				b: []byte("0123456789"),
			},
			want: &ByteBuffer{
				b: []byte("0123456789"),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewByteBuffer(tt.args.b); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewByteBuffer() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestByteBufferWriteAt(t *testing.T) {
	type args struct {
		p   []byte
		off int64
	}
	var buf ByteBuffer
	tests := []struct {
		args      args
		wantN     int
		wantErr   bool
		wantBytes []byte
	}{
		{
			args: args{
				off: -1,
			},
			wantErr: true,
		},
		{
			args: args{
				p: []byte{},
			},
		},
		{
			args: args{
				p:   []byte("89"),
				off: 8,
			},
			wantN:     2,
			wantBytes: []byte{0, 0, 0, 0, 0, 0, 0, 0, 56, 57},
		},
		{
			args: args{
				p:   []byte("0123"),
				off: 0,
			},
			wantN:     4,
			wantBytes: []byte{48, 49, 50, 51, 0, 0, 0, 0, 56, 57},
		},
		{
			args: args{
				p:   []byte("4567"),
				off: 4,
			},
			wantN:     4,
			wantBytes: []byte("0123456789"),
		},
	}
	for _, tt := range tests {
		n, err := buf.WriteAt(tt.args.p, tt.args.off)
		if (err != nil) != tt.wantErr {
			t.Errorf("error = %v, want = nil", err)
			return
		}
		if n != tt.wantN {
			t.Errorf("n = %d, want %d", n, tt.wantN)
			return
		}
		if gotBytes := buf.Bytes(); !reflect.DeepEqual(gotBytes, tt.wantBytes) {
			t.Errorf("bytes = %v, want %v", gotBytes, tt.wantBytes)
		}
	}
}

func TestByteBufferReadAt(t *testing.T) {
	buf := NewByteBuffer([]byte("0123456789"))
	tests := []struct {
		name    string
		off     int64
		size    int
		wantN   int
		wantErr bool
		wantP   []byte
	}{
		{
			name:  "EmptyBytes",
			wantP: []byte{},
		},
		{
			name:    "NegativeOffset",
			off:     -1,
			wantP:   []byte{},
			wantErr: true,
		},
		{
			name:  "Offset0",
			size:  10,
			wantN: 10,
			wantP: []byte("0123456789"),
		},
		{
			name:  "Offset4",
			size:  4,
			off:   4,
			wantN: 4,
			wantP: []byte("4567"),
		},
		{
			name:    "EOF",
			size:    4,
			off:     8,
			wantN:   2,
			wantErr: true,
			wantP:   []byte{56, 57, 0, 0},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := make([]byte, tt.size)
			n, err := buf.ReadAt(p, tt.off)
			if (err != nil) != tt.wantErr {
				t.Errorf("error = %v, want = nil", err)
				return
			}
			if n != tt.wantN {
				t.Errorf("n = %d, want %d", n, tt.wantN)
				return
			}
			if !reflect.DeepEqual(p, tt.wantP) {
				t.Errorf("p = %v, want %v", p, tt.wantP)
			}
		})
	}
}
