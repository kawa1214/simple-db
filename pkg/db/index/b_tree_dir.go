package index

import (
	"github.com/kawa1214/simple-db/pkg/db/file"
	"github.com/kawa1214/simple-db/pkg/db/record"
	"github.com/kawa1214/simple-db/pkg/db/tx"
)

type BTreeDir struct {
	tx       *tx.Transaction
	layout   *record.Layout
	contents *BTPage
	filename string
}

func NewBTreeDir(tx *tx.Transaction, blk *file.BlockID, layout *record.Layout) *BTreeDir {
	return &BTreeDir{
		tx:       tx,
		layout:   layout,
		contents: NewBTPage(tx, blk, layout),
		filename: blk.FileName(),
	}
}

func (dir *BTreeDir) Close() {
	dir.contents.Close()
}

func (dir *BTreeDir) Search(searchkey *record.Constant) int {
	childblk := dir.findChildBlock(searchkey)
	for dir.contents.GetFlag() > 0 {
		dir.contents.Close()
		dir.contents = NewBTPage(dir.tx, childblk, dir.layout)
		childblk = dir.findChildBlock(searchkey)
	}
	return childblk.Number()
}

func (dir *BTreeDir) MakeNewRoot(e *DirEntry) {
	firstval := dir.contents.GetDataVal(0)
	level := dir.contents.GetFlag()
	newblk := dir.contents.Split(0, level)
	oldroot := &DirEntry{firstval, newblk.Number()}
	dir.insertEntry(oldroot)
	dir.insertEntry(e)
	dir.contents.SetFlag(level + 1)
}

func (dir *BTreeDir) Insert(e *DirEntry) *DirEntry {
	if dir.contents.GetFlag() == 0 {
		return dir.insertEntry(e)
	}
	childblk := dir.findChildBlock(e.dataVal)
	child := NewBTreeDir(dir.tx, childblk, dir.layout)
	myentry := child.Insert(e)
	child.Close()
	if myentry != nil {
		return dir.insertEntry(myentry)
	}
	return nil
}

func (dir *BTreeDir) insertEntry(e *DirEntry) *DirEntry {
	newslot := 1 + dir.contents.FindSlotBefore(e.dataVal)
	dir.contents.InsertDir(newslot, e.dataVal, e.blockNum)
	if !dir.contents.IsFull() {
		return nil
	}
	level := dir.contents.GetFlag()
	splitpos := dir.contents.GetNumRecs() / 2
	splitval := dir.contents.GetDataVal(splitpos)
	newblk := dir.contents.Split(splitpos, level)
	return &DirEntry{dataVal: splitval, blockNum: newblk.Number()}
}

func (dir *BTreeDir) findChildBlock(searchkey *record.Constant) *file.BlockID {
	slot := dir.contents.FindSlotBefore(searchkey)
	if dir.contents.GetDataVal(slot + 1).Equals(searchkey) {
		slot++
	}
	blknum := dir.contents.GetChildNum(slot)
	return file.NewBlockID(dir.filename, blknum)
}
