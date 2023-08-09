package file

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

type FileMgr struct {
	dbDirectory string
	blocksize   int
	isNew       bool
	openFiles   map[string]*os.File
	mu          sync.Mutex
}

func NewFileMgr(dbDirectory string, blocksize int) *FileMgr {
	mgr := &FileMgr{
		dbDirectory: dbDirectory,
		blocksize:   blocksize,
		isNew:       !fileExists(dbDirectory),
		openFiles:   make(map[string]*os.File),
	}

	if mgr.isNew {
		os.MkdirAll(dbDirectory, 0755)
	}

	files, _ := filepath.Glob(filepath.Join(dbDirectory, "temp*"))
	for _, file := range files {
		os.Remove(file)
	}

	return mgr
}

func (mgr *FileMgr) Read(blk *BlockId, p *Page) error {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()

	f, err := mgr.getFile(blk.FileName())
	if err != nil {
		return fmt.Errorf("cannot read block %v: %w", blk, err)
	}

	_, err = f.Seek(int64(blk.Number()*mgr.blocksize), 0)
	if err != nil {
		return err
	}

	_, err = f.Read(p.Contents().Bytes())
	return err
}

func (mgr *FileMgr) Write(blk *BlockId, p *Page) error {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()

	f, err := mgr.getFile(blk.FileName())
	if err != nil {
		return fmt.Errorf("cannot write block %v: %w", blk, err)
	}

	_, err = f.Seek(int64(blk.Number()*mgr.blocksize), 0)
	if err != nil {
		return err
	}

	_, err = f.Write(p.Contents().Bytes())
	return err
}

func (mgr *FileMgr) Append(filename string) (*BlockId, error) {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()

	newblknum := mgr.size(filename)
	blk := NewBlockId(filename, newblknum)
	b := make([]byte, mgr.blocksize)

	f, err := mgr.getFile(blk.FileName())
	if err != nil {
		return nil, err
	}

	_, err = f.Seek(int64(blk.Number()*mgr.blocksize), 0)
	if err != nil {
		return nil, err
	}

	_, err = f.Write(b)
	if err != nil {
		return nil, err
	}

	return blk, nil
}

func (mgr *FileMgr) Length(filename string) (int, error) {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()

	f, err := mgr.getFile(filename)
	if err != nil {
		return 0, err
	}

	length, err := f.Seek(0, 2) // Seek to the end of the file
	return int(length) / mgr.blocksize, err
}

func (mgr *FileMgr) IsNew() bool {
	return mgr.isNew
}

func (mgr *FileMgr) BlockSize() int {
	return mgr.blocksize
}

func (mgr *FileMgr) getFile(filename string) (*os.File, error) {
	if f, exists := mgr.openFiles[filename]; exists {
		return f, nil
	}

	filePath := filepath.Join(mgr.dbDirectory, filename)
	f, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}

	mgr.openFiles[filename] = f
	return f, nil
}

func (mgr *FileMgr) size(filename string) int {
	f, err := mgr.getFile(filename)
	if err != nil {
		return -1
	}

	info, err := f.Stat()
	if err != nil {
		return -1
	}

	return int(info.Size()) / mgr.blocksize
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return !os.IsNotExist(err)
}
