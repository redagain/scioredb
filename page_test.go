package scioredb

import (
	"reflect"
	"testing"
	"time"
)

func TestPageWriteReadBool(t *testing.T) {
	var off int64
	p := NewPage(nil)
	want := []bool{true, false}
	for _, num := range want {
		n, err := p.WriteBool(num, off)
		if err != nil {
			t.Errorf("error = %v, want = nil", err)
			return
		}
		off += int64(n)
	}
	off = 0
	got := make([]bool, len(want))
	for i := 0; i < len(want); i++ {
		v, err := p.ReadBool(off)
		if err != nil {
			t.Errorf("error = %v, want = nil", err)
			return
		}
		off += 1
		got[i] = v
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestPageWriteReadInt(t *testing.T) {
	var off int64
	p := NewPage(nil)
	wantNumbers := []int{-100, -10, -1, 0, 1, 10, 100}
	for _, num := range wantNumbers {
		n, err := p.WriteInt(num, off)
		if err != nil {
			t.Errorf("error = %v, want = nil", err)
			return
		}
		off += int64(n)
	}
	off = 0
	gotNumbers := make([]int, len(wantNumbers))
	for i := 0; i < len(wantNumbers); i++ {
		v, err := p.ReadInt(off)
		if err != nil {
			t.Errorf("error = %v, want = nil", err)
			return
		}
		off += 8
		gotNumbers[i] = v
	}
	if !reflect.DeepEqual(gotNumbers, wantNumbers) {
		t.Errorf("got %v, want %v", gotNumbers, wantNumbers)
	}
}

func TestPageWriteReadString(t *testing.T) {
	type args struct {
		value string
		off   int64
		b     []byte
	}
	tests := []struct {
		name  string
		args  args
		wantN int
		want  string
	}{
		{
			name: "Nil",
			args: args{
				value: "12345",
				off:   0,
				b:     nil,
			},
			wantN: 13,
			want:  "12345",
		},
		{
			name: "OffIsGreaterThanZero",
			args: args{
				value: "12345",
				off:   20,
				b:     make([]byte, 50),
			},
			wantN: 13,
			want:  "12345",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewPage(tt.args.b)
			n, err := p.WriteString(tt.args.value, tt.args.off)
			if err != nil {
				t.Errorf("error = %v, want = nil", err)
				return
			}
			if tt.wantN != n {
				t.Errorf("gotN %v, wantN %v", n, tt.wantN)
				return
			}
			got, err := p.ReadString(tt.args.off)
			if err != nil {
				t.Errorf("error = %v, want = nil", err)
				return
			}
			if tt.want != got {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}
}

func TestPageManyWriteReadString(t *testing.T) {
	var (
		off     int64
		offsets []int64
		want    = "12345"
	)
	p := NewPage(make([]byte, 50))
	for i := 0; i < 2; i++ {
		offsets = append(offsets, off)
		n, err := p.WriteString(want, off)
		if err != nil {
			t.Errorf("error = %v, want = nil", err)
			return
		}
		off += int64(n)
	}
	for _, pos := range offsets {
		v, err := p.ReadString(pos)
		if err != nil {
			t.Errorf("error = %v, want = nil", err)
			return
		}
		if v != want {
			t.Errorf("got %v, want %v", v, want)
			return
		}
	}
}

func TestPageWriteReadTime(t *testing.T) {
	type args struct {
		value time.Time
		off   int64
		b     []byte
	}
	tests := []struct {
		name  string
		args  args
		wantN int
		want  time.Time
	}{
		{
			name: "Unix",
			args: args{
				value: time.Unix(0, 0),
				off:   10,
				b:     make([]byte, 50),
			},
			wantN: 23,
			want:  time.Unix(0, 0),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewPage(tt.args.b)
			n, err := p.WriteTime(tt.args.value, tt.args.off)
			if err != nil {
				t.Errorf("error = %v, want = nil", err)
				return
			}
			if tt.wantN != n {
				t.Errorf("gotN %v, wantN %v", n, tt.wantN)
				return
			}
			got, err := p.ReadTime(tt.args.off)
			if err != nil {
				t.Errorf("error = %v, want = nil", err)
				return
			}
			if tt.want != got {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}
}
