package scioredb

import (
	"io"
	"io/fs"
	"reflect"
	"slices"
	"testing"
)

func TestMemoryOpenedFileStat(t *testing.T) {
	f := &memoryOpenedFile{
		file: &memoryFile{
			name: "test.txt",
			data: NewByteBuffer([]byte("0123456789")),
		},
		mode: fs.FileMode(437),
	}
	gotInfo, err := f.Stat()
	if err != nil {
		t.Errorf("Stat() error = %v, want nil", err)
		return
	}
	wantInfo := &memoryFileInfo{
		name: "test.txt",
		size: 10,
		mode: fs.FileMode(437),
	}
	if !reflect.DeepEqual(gotInfo, wantInfo) {
		t.Errorf("Stat() gotInfo = %v, want %v", gotInfo, wantInfo)
	}
}

func TestMemoryOpenedFileWriteSeekReadClose(t *testing.T) {
	f := &memoryOpenedFile{
		file: &memoryFile{name: "test.txt", data: NewByteBuffer(nil)},
	}
	n, err := f.Write([]byte("0123456789"))
	if err != nil {
		t.Errorf("memoryOpenedFile.Write() error = %v, want nil", err)
		return
	}
	if n != 10 {
		t.Errorf("memoryOpenedFile.Write() got = %v, want %v", n, 10)
		return
	}
	pos, err := f.Seek(0, io.SeekStart)
	if err != nil {
		t.Errorf("memoryOpenedFile.Seek() error = %v, want nil", err)
		return
	}
	if pos != 0 {
		t.Errorf("memoryOpenedFile.Seek() got = %v, want %v", pos, 0)
		return
	}
	bts, err := io.ReadAll(f)
	if err != nil {
		t.Errorf("memoryOpenedFile.Read() error = %v, want nil", err)
		return
	}
	if string(bts) != "0123456789" {
		t.Errorf("memoryOpenedFile.Read() got = %v, want %v", string(bts), "0123456789")
		return
	}
	err = f.Close()
	if err != nil {
		t.Errorf("memoryOpenedFile.Close() error = %v, want nil", err)
	}
}

func TestMemoryOpenedFileWriteReadAtClose(t *testing.T) {
	f := &memoryOpenedFile{
		file: &memoryFile{name: "test.txt", data: NewByteBuffer([]byte("9876543210"))},
	}
	n, err := f.WriteAt([]byte("0123456789"), 0)
	if err != nil {
		t.Errorf("memoryOpenedFile.WriteAt() error = %v, want nil", err)
		return
	}
	if n != 10 {
		t.Errorf("memoryOpenedFile.WriteAt() got = %v, want %v", n, 10)
		return
	}
	p := make([]byte, 9)
	n, err = f.ReadAt(p, 1)
	if err != nil {
		t.Errorf("memoryOpenedFile.ReadAt() error = %v, want nil", err)
		return
	}
	if n != 9 {
		t.Errorf("memoryOpenedFile.ReadAt() got = %v, want %v", n, 9)
		return
	}
	if string(p) != "123456789" {
		t.Errorf("memoryOpenedFile.ReadAt() got = %v, want %v", string(p), "123456789")
		return
	}
	_, err = f.ReadAt(p, 9)
	if err == nil {
		t.Errorf("memoryOpenedFile.WriteAt() error = %v, want %v", err, io.EOF)
		return
	}
	err = f.Close()
	if err != nil {
		t.Errorf("memoryOpenedFile.Close() error = %v, want nil", err)
	}
}

func TestOpenManyMemoryFile(t *testing.T) {
	file := &memoryFile{name: "test.txt", data: NewByteBuffer([]byte("0123456789"))}
	f1 := &memoryOpenedFile{
		file: file,
	}
	f2 := &memoryOpenedFile{
		file: file,
	}
	_, err := f1.Write([]byte("0123456789"))
	if err != nil {
		t.Errorf("f1.Write() error = %v, want nil", err)
		return
	}
	stat, err := f2.Stat()
	if err != nil {
		t.Errorf("f2.Stat() error = %v, want nil", err)
		return
	}
	if stat.Size() != 10 {
		t.Errorf("f2.Size() got = %v, want %v", stat.Size(), 10)
		return
	}
	bts, err := io.ReadAll(f2)
	if err != nil {
		t.Errorf("f1.ReadAll() error = %v, want nil", err)
		return
	}
	if string(bts) != "0123456789" {
		t.Errorf("f1.ReadAll() got = %v, want %v", string(bts), "0123456789")
		return
	}
	_, err = f1.Seek(0, io.SeekStart)
	if err != nil {
		t.Errorf("f1.Seek() error = %v, want nil", err)
		return
	}
	bts, err = io.ReadAll(f1)
	if string(bts) != "0123456789" {
		t.Errorf("f1.ReadAll() got = %v, want %v", string(bts), "0123456789")
	}
}

func TestMemoryFileClose(t *testing.T) {
	tests := []struct {
		name  string
		after func(f *memoryOpenedFile) error
	}{
		{
			name: "Close",
			after: func(f *memoryOpenedFile) error {
				return f.Close()
			},
		},
		{
			name: "Stat",
			after: func(f *memoryOpenedFile) (err error) {
				_, err = f.Stat()
				return err
			},
		},
		{
			name: "Write",
			after: func(f *memoryOpenedFile) (err error) {
				_, err = f.Write(nil)
				return err
			},
		},
		{
			name: "WriteAt",
			after: func(f *memoryOpenedFile) (err error) {
				_, err = f.WriteAt(make([]byte, 4), 0)
				return err
			},
		},
		{
			name: "Seek",
			after: func(f *memoryOpenedFile) (err error) {
				_, err = f.Seek(0, io.SeekStart)
				return err
			},
		},
		{
			name: "Read",
			after: func(f *memoryOpenedFile) (err error) {
				_, err = f.Read(nil)
				return err
			},
		},
		{
			name: "ReadAt",
			after: func(f *memoryOpenedFile) (err error) {
				_, err = f.ReadAt(make([]byte, 4), 0)
				return err
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := &memoryOpenedFile{file: &memoryFile{name: "test.txt", data: NewByteBuffer(nil)}}
			err := f.Close()
			if err != nil {
				t.Errorf("error = %v, want = nil", err)
			}
			if tt.after != nil {
				err = tt.after(f)
				if err == nil {
					t.Errorf("error = nil, want error %v", fs.ErrClosed)
				}
			}
		})
	}
}

func TestMemoryFSOpen(t *testing.T) {
	type args struct {
		name  string
		files []MemoryFile
	}
	tests := []struct {
		name     string
		args     args
		wantFile string
	}{
		{
			name: "OpenFile",
			args: args{
				name:  "test.txt",
				files: []MemoryFile{{Name: "test.txt"}},
			},
			wantFile: "test.txt",
		},
		{
			name: "CreateFile",
			args: args{
				name: "test.txt",
			},
			wantFile: "test.txt",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := MemoryFS(tt.args.files...)
			f, err := s.Open(tt.args.name)
			if err != nil {
				t.Errorf("MemoryFS.Open() error = %v, want nil", err)
				return
			}
			if f == nil {
				t.Errorf("MemoryFS.Open() file = nil, want not nil")
				return
			}
			if f.Name() != tt.wantFile {
				t.Errorf("MemoryFS.Open() file with name %v was expected", tt.wantFile)
			}
		})
	}
}

func TestMemoryFSGlob(t *testing.T) {
	type args struct {
		pattern string
		files   []MemoryFile
	}
	tests := []struct {
		name      string
		args      args
		wantFiles []string
		wantErr   bool
	}{
		{
			name: "Empty",
			args: args{
				pattern: "",
				files:   []MemoryFile{{Name: "test.txt"}},
			},
		},
		{
			name: "test.*",
			args: args{
				pattern: "test.*",
				files:   []MemoryFile{{Name: "test.txt"}, {Name: "test.dat"}, {Name: "1.txt"}},
			},
			wantFiles: []string{"test.txt", "test.dat"},
		},
		{
			name: "*.txt",
			args: args{
				pattern: "*.txt",
				files:   []MemoryFile{{Name: "test1.txt"}, {Name: "test2.txt"}, {Name: "test.dat"}},
			},
			wantFiles: []string{"test1.txt", "test2.txt"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := MemoryFS(tt.args.files...)
			gotFiles, err := s.Glob(tt.args.pattern)
			if (err != nil) != tt.wantErr {
				t.Errorf("MemoryFS.Glob() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			for _, wantFile := range tt.wantFiles {
				if !slices.Contains(gotFiles, wantFile) {
					t.Errorf("MemoryFS.Glob() gotFiles = %v, want %v", gotFiles, tt.wantFiles)
				}
			}
		})
	}
}

func TestMemoryFSRemove(t *testing.T) {
	s := MemoryFS(MemoryFile{
		Name: "test.txt",
	})
	err := s.Remove("test1.txt")
	if err == nil {
		t.Errorf("MemoryFS.Remove() got nil, want error %v", errNoSuchFileOrDirectory)
		return
	}
	err = s.Remove("test.txt")
	if err != nil {
		t.Errorf("MemoryFS.Remove() error = %v, want nil", err)
	}
}
