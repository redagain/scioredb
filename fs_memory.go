package scioredb

import (
	"errors"
	"io"
	"io/fs"
	"maps"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

const (
	opMemoryFileWrite   = "write"
	opMemoryFileWriteAt = "writeat"
	opMemoryFileReadAt  = "readat"
	opMemoryFileRead    = "read"
	opMemoryFileSeek    = "seek"
	opMemoryFileStat    = "stat"
	opMemoryFileClose   = "close"
	opMemoryFileRemove  = "remove"
)

type memoryFileInfo struct {
	size    int64
	name    string
	mode    fs.FileMode
	modTime time.Time
}

var _ fs.FileInfo = &memoryFileInfo{}

func (i *memoryFileInfo) Name() string { return i.name }

func (i *memoryFileInfo) Size() int64 { return i.size }

func (i *memoryFileInfo) Mode() fs.FileMode { return i.mode }

func (i *memoryFileInfo) ModTime() time.Time { return i.modTime }

func (i *memoryFileInfo) IsDir() bool { return false }

func (i *memoryFileInfo) Sys() any { return nil }

type memoryFile struct {
	name    string
	size    atomic.Int64
	modTime time.Time
	data    *ByteBuffer
	mu      sync.RWMutex
}

func (f *memoryFile) Name() string { return f.name }

func (f *memoryFile) Size() int64 { return f.size.Load() }

func (f *memoryFile) WriteAt(p []byte, off int64) (n int, err error) {
	f.mu.Lock()
	n, err = f.data.WriteAt(p, off)
	f.size.Store(int64(f.data.Len()))
	f.modTime = time.Now()
	f.mu.Unlock()
	return
}

func (f *memoryFile) ReadAt(p []byte, off int64) (n int, err error) {
	f.mu.RLock()
	n, err = f.data.ReadAt(p, off)
	f.mu.RUnlock()
	return
}

func (f *memoryFile) Stat() (info *memoryFileInfo) {
	f.mu.RLock()
	info = &memoryFileInfo{
		name:    f.name,
		size:    int64(f.data.Len()),
		modTime: f.modTime,
	}
	f.mu.RUnlock()
	return
}

type memoryOpenedFile struct {
	off    int64
	mode   fs.FileMode
	file   *memoryFile
	closed atomic.Bool
	mu     sync.Mutex
}

func (f *memoryOpenedFile) checkClosed(op string) error {
	if f.closed.Load() {
		return &fs.PathError{Op: op, Path: f.Name(), Err: fs.ErrClosed}
	}
	return nil
}

func (f *memoryOpenedFile) Name() string { return f.file.Name() }

func (f *memoryOpenedFile) Stat() (fs.FileInfo, error) {
	if err := f.checkClosed(opMemoryFileStat); err != nil {
		return nil, err
	}
	stat := f.file.Stat()
	stat.mode = f.mode
	return stat, nil
}

func (f *memoryOpenedFile) Write(p []byte) (n int, err error) {
	if err = f.checkClosed(opMemoryFileWrite); err != nil {
		return
	}
	if len(p) == 0 {
		return
	}
	f.mu.Lock()
	n, err = f.file.WriteAt(p, f.off)
	if err == nil {
		f.off += int64(n)
	}
	f.mu.Unlock()
	if err != nil {
		err = &fs.PathError{Op: opMemoryFileWrite, Path: f.Name(), Err: err}
	}
	return
}

func (f *memoryOpenedFile) WriteAt(p []byte, off int64) (n int, err error) {
	if err = f.checkClosed(opMemoryFileWriteAt); err != nil {
		return
	}
	n, err = f.file.WriteAt(p, off)
	if err != nil {
		err = &fs.PathError{Op: opMemoryFileWriteAt, Path: f.file.Name(), Err: err}
	}
	return
}

func (f *memoryOpenedFile) Seek(offset int64, whence int) (pos int64, err error) {
	if err = f.checkClosed(opMemoryFileSeek); err != nil {
		return
	}
	f.mu.Lock()
	genSeeker := genericSeeker{
		Off:  f.off,
		Size: f.file.Size(),
	}
	pos, err = genSeeker.Seek(offset, whence)
	if err == nil {
		f.off = pos
	}
	f.mu.Unlock()
	if err != nil {
		err = &fs.PathError{Op: opMemoryFileSeek, Path: f.file.Name(), Err: err}
	}
	return
}

func (f *memoryOpenedFile) Read(p []byte) (n int, err error) {
	if err = f.checkClosed(opMemoryFileRead); err != nil {
		return
	}
	if len(p) == 0 {
		return
	}
	f.mu.Lock()
	n, err = f.file.ReadAt(p, f.off)
	f.off += int64(n)
	f.mu.Unlock()
	if err != nil && !errors.Is(err, io.EOF) {
		err = &fs.PathError{Op: opMemoryFileRead, Path: f.Name(), Err: err}
	}
	return
}

func (f *memoryOpenedFile) ReadAt(p []byte, off int64) (n int, err error) {
	if err = f.checkClosed(opMemoryFileReadAt); err != nil {
		return
	}
	n, err = f.file.ReadAt(p, off)
	if err != nil && !errors.Is(err, io.EOF) {
		err = &fs.PathError{Op: opMemoryFileReadAt, Path: f.file.Name(), Err: err}
	}
	return
}

func (f *memoryOpenedFile) Close() (err error) {
	if err = f.checkClosed(opMemoryFileClose); err != nil {
		return
	}
	f.closed.Store(true)
	return
}

var errNoSuchFileOrDirectory = errors.New("no such file or directory")

type MemoryFile struct {
	Name string
	Data []byte
}

type memoryFS struct {
	files map[string]*memoryFile
	mu    sync.Mutex
}

func MemoryFS(file ...MemoryFile) FS {
	s := &memoryFS{
		files: make(map[string]*memoryFile),
	}
	for _, f := range file {
		if f.Name != "" {
			s.files[f.Name] = &memoryFile{
				name: f.Name,
				data: NewByteBuffer(f.Data),
			}
		}
	}
	return s
}

func (s *memoryFS) openOrCreateFile(name string) (f File, err error) {
	filename := filepath.Base(name)
	file, ok := s.files[filename]
	if !ok {
		file = &memoryFile{
			name:    filename,
			data:    NewByteBuffer(make([]byte, 0, 4)),
			modTime: time.Now(),
		}
		s.files[filename] = file
	}
	f = &memoryOpenedFile{file: file}
	return
}

func (s *memoryFS) Open(name string) (f File, err error) {
	s.mu.Lock()
	f, err = s.openOrCreateFile(name)
	s.mu.Unlock()
	return
}

func (s *memoryFS) Glob(pattern string) (files []string, err error) {
	s.mu.Lock()
	fileNames := maps.Keys(s.files)
	var match bool
	for file := range fileNames {
		match, err = filepath.Match(pattern, file)
		if err != nil {
			return
		}
		if match {
			files = append(files, file)
		}
	}
	s.mu.Unlock()
	return
}

func (s *memoryFS) Remove(name string) (err error) {
	s.mu.Lock()
	_, ok := s.files[name]
	if ok {
		delete(s.files, name)
	}
	s.mu.Unlock()
	if !ok {
		err = &fs.PathError{Op: opMemoryFileRemove, Path: name, Err: errNoSuchFileOrDirectory}
	}
	return
}
