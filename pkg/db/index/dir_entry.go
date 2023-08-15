package index

import "github.com/kawa1214/simple-db/pkg/db/record"

type DirEntry struct {
	dataVal  *record.Constant
	blockNum int
}

func NewDirEntry(dataVal *record.Constant, blockNum int) *DirEntry {
	return &DirEntry{
		dataVal:  dataVal,
		blockNum: blockNum,
	}
}

func (entry *DirEntry) DataVal() *record.Constant {
	return entry.dataVal
}

func (entry *DirEntry) BlockNumber() int {
	return entry.blockNum
}
