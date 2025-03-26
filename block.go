package scioredb

const BlockSize = 512

type Block struct {
	ID       int
	FileName string
}

func BlockID(fileSize, blockSize int64) int64 { return fileSize / blockSize }
