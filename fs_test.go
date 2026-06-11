package scioredb

import (
	"bytes"
	"io"
	"path/filepath"
	"sync"
	"testing"
)

func TestMemFile(t *testing.T) {
	var (
		name  = "test.dat"
		dataA = []byte("AAAAA")
		dataB = []byte("BBBBB")
	)
	f := NewMemFile(name, dataA)
	if f.Name() != name {
		t.Errorf("Name mismatch, expected name %s", name)
		return
	}
	size, err := f.Size()
	if err != nil {
		t.Errorf("Size() error = %v", err)
		return
	}
	if size != int64(len(dataA)) {
		t.Errorf("Size mismatch, expected size %d", len(dataA))
		return
	}
	p := make([]byte, len(dataA))
	n, err := f.ReadAt(p, 0)
	if err != nil {
		t.Errorf("ReadAt() error = %v", err)
		return
	}
	if n != 5 {
		t.Errorf("ReadAt() returned %d bytes, expected 5", n)
		return
	}
	if !bytes.Equal(p, dataA) {
		t.Errorf("ReadAt() returned bytes %x, expected %x", p, dataA)
		return
	}
	n, err = f.WriteAt(dataB, 5)
	if err != nil {
		t.Errorf("WriteAt() error = %v", err)
		return
	}
	if n != 5 {
		t.Errorf("WriteAt() returned %d bytes, expected 5", n)
		return
	}
	p = make([]byte, 11)
	n, err = f.ReadAt(p, 0)
	if err == nil {
		t.Errorf("ReadAt() error = %v, expected error", n)
		return
	}
	if n != 10 {
		t.Errorf("ReadAt() returned %d bytes, expected 10", n)
		return
	}
	data := append(dataA, dataB...)
	if !bytes.HasPrefix(p, data) {
		t.Errorf("ReadAt() returned bytes %x, expected %x", p, data)
	}
}

func TestMemFileConcurrentWriteReadAt(t *testing.T) {
	f := NewMemFile("test.dat", []byte("AAAAA"))
	dataA := []byte("AAAAA")
	dataB := []byte("BBBBB")
	var wg sync.WaitGroup
	done := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			if i%2 == 0 {
				_, _ = f.WriteAt(dataA, 0)
			} else {
				_, _ = f.WriteAt(dataB, 0)
			}
		}
		close(done)
	}()
	readersCount := 30
	for i := 0; i < readersCount; i++ {
		wg.Add(1)
		go func(readerID int) {
			defer wg.Done()

			buf := make([]byte, 5)

			for {
				select {
				case <-done:
					return
				default:
					_, err := f.ReadAt(buf, 0)
					if err != nil && err != io.EOF {
						t.Errorf("Reader %d ReadAt() error = %v", readerID, err)
						return
					}
					if !bytes.Equal(buf, dataA) && !bytes.Equal(buf, dataB) {
						t.Errorf("Reader %d detected data corruption! Buffer: %q", readerID, buf)
						return
					}
				}
			}
		}(i)
	}
	wg.Wait()
}

func TestMemFileDescriptor(t *testing.T) {
	fd := NewMemFileDescriptor(NewMemFile("test.dat", []byte("AAAAA")))
	err := fd.Close()
	if err != nil {
		t.Errorf("Close() error = %v", err)
		return
	}
	_, err = fd.WriteAt([]byte{0}, 0)
	if err == nil {
		t.Error("WriteAt() - file already closed, error expected")
		return
	}
	_, err = fd.ReadAt([]byte{0}, 0)
	if err == nil {
		t.Error("ReadAt() - file already closed, error expected")
		return
	}
	_, err = fd.Size()
	if err == nil {
		t.Error("Size() - file already closed, error expected")
		return
	}
	err = fd.Close()
	if err == nil {
		t.Error("Close() - file already closed, error expected")
		return
	}
}

func TestMemFS(t *testing.T) {
	type fileData struct {
		Name string
		Data []byte
	}
	var (
		file1 = fileData{
			Name: "test1.dat",
			Data: []byte("AAAAA"),
		}
		file2 = fileData{
			Name: "test2.dat",
			Data: []byte("BBBBB"),
		}
	)
	fsys := NewMemFS(NewMemFile(file1.Name, file1.Data))
	f1, err := fsys.Open(file1.Name)
	if err != nil {
		t.Errorf("Open() error = %v", err)
		return
	}
	if f1.Name() != file1.Name {
		t.Errorf("Name mismatch, expected %s got %s", file1.Name, f1.Name())
		return
	}
	f2, err := fsys.Open(filepath.Join("data", file2.Name))
	if err != nil {
		t.Errorf("Open() error = %v", err)
		return
	}
	if f2.Name() != file2.Name {
		t.Errorf("Name mismatch, expected %s got %s", file2.Name, f2.Name())
		return
	}
	_, _ = f2.WriteAt(file2.Data, 0)
	p := make([]byte, 5)
	_, _ = f1.ReadAt(p, 0)
	if !bytes.Equal(p, file1.Data) {
		t.Error("First file data was corrupted after being written to second")
	}
}

func TestMemFSConcurrentOpen(t *testing.T) {
	const (
		openCount  = 10
		filesCount = 2
	)
	var wg sync.WaitGroup
	wg.Add(openCount)
	fsys := NewMemFS()
	for i := 0; i < openCount; i++ {
		name := "test1.dat"
		if i%2 == 0 {
			name = "test2.dat"
		}
		go func(filename string) {
			defer wg.Done()
			_, err := fsys.Open(filename)
			if err != nil {
				t.Errorf("Open() error = %v", err)
				return
			}
		}(name)
	}
	wg.Wait()
	if fsys.Len() != filesCount {
		t.Errorf("Open() returned %d files, expected %d", fsys.Len(), filesCount)
	}
}
