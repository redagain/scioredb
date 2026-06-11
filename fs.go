package scioredb

import (
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
)

type File interface {
	Name() string
	WriteAt(p []byte, off int64) (n int, err error)
	ReadAt(p []byte, off int64) (n int, err error)
	Size() (int64, error)
	Close() error
}

type FS interface {
	Open(name string) (File, error)
}

type osFile struct {
	*os.File
}

func (f *osFile) Size() (int64, error) {
	info, err := f.Stat()
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

type DirFS string

func (dir DirFS) Open(name string) (File, error) {
	if dir == "" {
		return nil, errors.New("DirFS empty root")
	}
	filename := filepath.Join(string(dir), filepath.Base(name))
	f, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0)
	if err != nil {
		return nil, &fs.PathError{Op: "open", Path: name, Err: err}
	}
	return &osFile{f}, nil
}

type MemFile struct {
	name string
	data *ByteBuffer
	mu   sync.RWMutex
}

func NewMemFile(name string, data []byte) *MemFile {
	return &MemFile{
		name: filepath.Base(name),
		data: NewByteBuffer(data),
	}
}

func (f *MemFile) Name() string {
	return f.name
}

func (f *MemFile) WriteAt(p []byte, off int64) (n int, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	n, err = f.data.WriteAt(p, off)
	if err != nil {
		if errors.Is(err, ErrNegativeOffset) {
			return 0, &fs.PathError{Op: "writeat", Path: f.name, Err: ErrNegativeOffset}
		}
		return 0, &fs.PathError{Op: "write", Path: f.name, Err: err}
	}
	return
}

func (f *MemFile) ReadAt(p []byte, off int64) (n int, err error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	n, err = f.data.ReadAt(p, off)
	if err != nil {
		switch {
		case errors.Is(err, io.EOF):
			return n, err
		case errors.Is(err, ErrNegativeOffset):
			return 0, &fs.PathError{Op: "readat", Path: f.name, Err: ErrNegativeOffset}
		default:
			return 0, &fs.PathError{Op: "read", Path: f.name, Err: err}
		}
	}
	return
}

func (f *MemFile) Size() (size int64, err error) {
	f.mu.RLock()
	size = int64(f.data.Len())
	f.mu.RUnlock()
	return
}

type MemFileDescriptor struct {
	file   *MemFile
	closed atomic.Bool
}

func NewMemFileDescriptor(file *MemFile) *MemFileDescriptor {
	return &MemFileDescriptor{
		file: file,
	}
}

func (f *MemFileDescriptor) Name() string {
	return f.file.Name()
}

func (f *MemFileDescriptor) WriteAt(p []byte, off int64) (n int, err error) {
	if f.closed.Load() {
		return 0, &fs.PathError{Op: "writeat", Path: f.file.Name(), Err: fs.ErrClosed}
	}
	return f.file.WriteAt(p, off)
}

func (f *MemFileDescriptor) ReadAt(p []byte, off int64) (n int, err error) {
	if f.closed.Load() {
		return 0, &fs.PathError{Op: "readat", Path: f.file.Name(), Err: fs.ErrClosed}
	}
	return f.file.ReadAt(p, off)
}

func (f *MemFileDescriptor) Size() (size int64, err error) {
	if f.closed.Load() {
		return 0, &fs.PathError{Op: "size", Path: f.file.Name(), Err: fs.ErrClosed}
	}
	return f.file.Size()
}

func (f *MemFileDescriptor) Close() error {
	if f.closed.Load() {
		return &fs.PathError{Op: "close", Path: f.file.Name(), Err: fs.ErrClosed}
	}
	f.closed.Store(true)
	return nil
}

type MemFS struct {
	files map[string]*MemFile
	mu    sync.RWMutex
}

func NewMemFS(file ...*MemFile) *MemFS {
	files := make(map[string]*MemFile)
	for _, f := range file {
		files[f.Name()] = f
	}
	return &MemFS{files: files}
}

func (fsys *MemFS) Open(name string) (File, error) {
	filename := filepath.Base(name)
	fsys.mu.RLock()
	f, ok := fsys.files[filename]
	fsys.mu.RUnlock()
	if ok {
		return NewMemFileDescriptor(f), nil
	}
	fsys.mu.Lock()
	defer fsys.mu.Unlock()
	if f, ok = fsys.files[filename]; ok {
		return NewMemFileDescriptor(f), nil
	}
	f = NewMemFile(filename, make([]byte, 0))
	fsys.files[filename] = f
	return NewMemFileDescriptor(f), nil
}

func (fsys *MemFS) Len() int {
	fsys.mu.RLock()
	defer fsys.mu.RUnlock()
	return len(fsys.files)
}
