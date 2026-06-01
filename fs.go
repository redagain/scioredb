package scioredb

import (
	"io"
	"io/fs"
)

type File interface {
	io.Closer
	io.Seeker
	io.WriterAt
	io.Writer
	io.Reader
	io.ReaderAt
	Name() string
	Stat() (fs.FileInfo, error)
}

type FS interface {
	Open(name string) (File, error)
}
