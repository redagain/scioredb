package scioredb

import (
	"fmt"
	"slices"
	"testing"
	"time"
)

// Original test source - https://github.com/eatonphil/sciore-simpledb-3.4/blob/main/SimpleDB_3.4/simpledb/file/FileTest.java
func TestFileManager(t *testing.T) {
	var (
		blockSize       = 100
		off       int64 = 11
	)
	p1 := NewPage(make([]byte, blockSize))
	fs := MemoryFS()
	fm := NewFileManager(fs, WithBlockSize(blockSize))
	b := Block{
		ID:       2,
		FileName: "test.sdbf",
	}
	values := []any{true, false, "12345", 345, time.Unix(0, 0)}
	for _, v := range values {
		var (
			n   int
			err error
		)
		switch val := v.(type) {
		case bool:
			n, err = p1.WriteBool(val, off)
		case int:
			n, err = p1.WriteInt(val, off)
		case string:
			n, err = p1.WriteString(val, off)
		case time.Time:
			n, err = p1.WriteTime(val, off)
		}
		if err != nil {
			t.Errorf("error = %v, want = nil", err)
			return
		}
		off += int64(n)
	}
	err := fm.Write(b, p1)
	if err != nil {
		t.Errorf("error = %v, want = nil", err)
		return
	}
	p2, err := fm.Read(b)
	if err != nil {
		t.Errorf("error = %v, want = nil", err)
		return
	}
	if !slices.Equal(p1.Bytes(), p2.Bytes()) {
		t.Errorf("p1.Bytes() = %v, p2.Bytes() = %v", p1.Bytes(), p2.Bytes())
	}
}

func TestFileManagerAppend(t *testing.T) {
	fm := NewFileManager(MemoryFS())
	want := Block{
		ID:       0,
		FileName: "test.sdbf",
	}
	got, err := fm.Append("test.sdbf")
	if err != nil {
		t.Errorf("error = %v, want = nil", err)
		return
	}
	if want != got {
		t.Errorf("want = %v, got = %v", want, got)
	}
}

func TestFileManagerClose(t *testing.T) {
	type args struct {
		files []MemoryFile
	}
	tests := []struct {
		name   string
		args   args
		before func(m *FileManager)
		after  func(m *FileManager) error
	}{
		{
			name: "NoOpenFiles",
			after: func(m *FileManager) error {
				return m.Close()
			},
		},
		{
			name: "Close",
			before: func(m *FileManager) {
				for i := 1; i < 5; i++ {
					_, _ = m.Append(fmt.Sprintf("test-%d.sdbf", i))
				}
			},
			after: func(m *FileManager) error {
				return m.Close()
			},
		},
		{
			name: "Append",
			after: func(m *FileManager) error {
				_, err := m.Append("test.sdbf")
				return err
			},
		},
		{
			name: "Write",
			after: func(m *FileManager) error {
				err := m.Write(Block{
					ID:       1,
					FileName: "test.sdbf",
				}, NewPage(make([]byte, 10)))
				return err
			},
		},
		{
			name: "Raed",
			after: func(m *FileManager) error {
				_, err := m.Read(Block{
					ID:       1,
					FileName: "test.sdbf",
				})
				return err
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fm := NewFileManager(MemoryFS(tt.args.files...))
			if tt.before != nil {
				tt.before(fm)
			}
			err := fm.Close()
			if err != nil {
				t.Errorf("error = %v, want = nil", err)
				return
			}
			if tt.after != nil {
				err := tt.after(fm)
				if err == nil {
					t.Errorf("error = nil, want error %v", errFileManagerClosed)
				}
			}
		})
	}
}
