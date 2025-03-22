package scioredb

import (
	"errors"
	"io"
)

var (
	errSeekInvalidArgument  = errors.New("invalid argument")
	errSeekNegativePosition = errors.New("negative position")
)

type genericSeeker struct {
	Off  int64
	Size int64
}

func (s *genericSeeker) Seek(offset int64, whence int) (pos int64, err error) {
	switch whence {
	case io.SeekStart:
		pos = offset
	case io.SeekCurrent:
		pos = offset + s.Off
	case io.SeekEnd:
		pos = offset + s.Size
	default:
		err = errSeekInvalidArgument
		return
	}
	if pos < 0 {
		err = errSeekNegativePosition
	}
	return
}
