package scioredb

import (
	"errors"
	"io"
	"sync"
	"sync/atomic"
)

var errFileManagerClosed = errors.New("file manager already closed")

type FileManager struct {
	blockSize int
	fs        FS
	openFiles map[string]File
	closed    atomic.Bool
	mu        sync.RWMutex
}

func NewFileManager(fs FS, blockSize int) *FileManager {
	return &FileManager{
		blockSize: blockSize,
		fs:        fs,
		openFiles: make(map[string]File),
	}
}

func (m *FileManager) getFile(name string) (f File, isNew bool, err error) {
	var ok bool
	f, ok = m.openFiles[name]
	if !ok {
		f, err = m.fs.Open(name)
		if err != nil {
			return
		}
		m.openFiles[name] = f
		isNew = true
	}
	return
}

func (m *FileManager) writePage(block Block, page *Page) error {
	f, _, err := m.getFile(block.FileName)
	if err != nil {
		return err
	}
	off := int64(block.ID * m.blockSize)
	_, err = f.WriteAt(page.Bytes(), off)
	return err
}

func (m *FileManager) readPage(block Block) (*Page, error) {
	f, _, err := m.getFile(block.FileName)
	if err != nil {
		return nil, err
	}
	b := make([]byte, m.blockSize)
	off := int64(block.ID * m.blockSize)
	_, err = f.ReadAt(b, off)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	return NewPage(b), err
}

func (m *FileManager) checkClosed() error {
	if m.closed.Load() {
		return errFileManagerClosed
	}
	return nil
}

func (m *FileManager) Write(block Block, page *Page) (err error) {
	if err = m.checkClosed(); err != nil {
		return err
	}
	m.mu.Lock()
	err = m.writePage(block, page)
	m.mu.Unlock()
	return err
}

func (m *FileManager) Read(block Block) (p *Page, err error) {
	if err = m.checkClosed(); err != nil {
		return
	}
	m.mu.RLock()
	p, err = m.readPage(block)
	m.mu.RUnlock()
	return
}

func (m *FileManager) appendFile(name string) (Block, error) {
	f, isNew, err := m.getFile(name)
	if err != nil {
		return Block{}, err
	}
	if isNew {
		return Block{FileName: name}, nil
	}
	stat, err := f.Stat()
	if err != nil {
		return Block{}, err
	}
	n := BlockID(stat.Size(), int64(m.blockSize))
	return Block{
		ID:       int(n),
		FileName: name,
	}, nil
}

func (m *FileManager) Append(filename string) (b Block, err error) {
	if err = m.checkClosed(); err != nil {
		return
	}
	m.mu.Lock()
	b, err = m.appendFile(filename)
	m.mu.Unlock()
	return
}

func (m *FileManager) close() (err error) {
	if len(m.openFiles) == 0 {
		return
	}
	var (
		wg   sync.WaitGroup
		errs = make([]error, 0, len(m.openFiles))
	)
	for _, file := range m.openFiles {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := file.Close(); err != nil {
				errs = append(errs, err)
			}
		}()
	}
	wg.Wait()
	return errors.Join(errs...)
}

func (m *FileManager) Close() error {
	if m.closed.Load() {
		return errFileManagerClosed
	}
	m.closed.Store(true)
	m.mu.Lock()
	err := m.close()
	m.mu.Unlock()
	return err
}
