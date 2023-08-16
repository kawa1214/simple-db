package log

import "github.com/kawa1214/simple-db/pkg/db/file"

type LogIterator struct {
	fm         *file.FileMgr
	blk        *file.BlockID
	p          *file.Page
	currentpos int
	boundary   int
}

func NewLogIterator(fm *file.FileMgr, blk *file.BlockID) *LogIterator {
	page := file.NewPage(fm.BlockSize())
	li := &LogIterator{
		fm:  fm,
		blk: blk,
		p:   page,
	}
	li.moveToBlock(blk)
	return li
}

func (li *LogIterator) HasNext() bool {
	return li.currentpos < li.fm.BlockSize() || li.blk.Number() > 0
}

func (li *LogIterator) Next() []byte {
	if li.currentpos == li.fm.BlockSize() {
		blockId := file.NewBlockID(li.blk.FileName(), li.blk.Number()-1)
		li.blk = blockId
		li.moveToBlock(li.blk)
	}
	rec := li.p.GetBytes(li.currentpos)
	li.currentpos += 4 + len(rec) // assuming int is 4 bytes
	return rec
}

func (li *LogIterator) moveToBlock(blk *file.BlockID) {
	li.fm.Read(blk, li.p)
	li.boundary = li.p.GetInt(0)
	li.currentpos = li.boundary
}
