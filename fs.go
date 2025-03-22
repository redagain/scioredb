package scioredb

import (
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
)

type File interface {
	io.Closer
	io.Writer
	io.Reader
	io.Seeker
	io.WriterAt
	io.ReaderAt
	Name() string
	Stat() (fs.FileInfo, error)
}

type FS interface {
	Open(name string) (File, error)
	Glob(pattern string) ([]string, error)
	Remove(name string) error
}

var errDirFSEmptyRoot = errors.New("DirFS with empty root")

type dirFS string

func DirFS(dir string) FS {
	return dirFS(dir)
}

func (dir dirFS) Open(name string) (File, error) {
	if dir == "" {
		return nil, errDirFSEmptyRoot
	}
	filename := filepath.Join(string(dir), name)
	f, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0)
	var pathErr *fs.PathError
	if err != nil && errors.As(err, &pathErr) {
		pathErr.Path = name
	}
	return f, err
}

func (dir dirFS) Glob(pattern string) ([]string, error) {
	if dir == "" {
		return nil, errDirFSEmptyRoot
	}
	return filepath.Glob(filepath.Join(string(dir), pattern))
}

func (dir dirFS) Remove(name string) error {
	if dir == "" {
		return errDirFSEmptyRoot
	}
	filename := filepath.Join(string(dir), name)
	return os.Remove(filename)
}
