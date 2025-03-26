package scioredb

import (
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
	fm := NewFileManager(fs, blockSize)
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
