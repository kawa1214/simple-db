package multibuffer

import (
	"github.com/kawa1214/simple-db/pkg/db/file"
	"github.com/kawa1214/simple-db/pkg/db/record"
	"github.com/kawa1214/simple-db/pkg/db/tx"
)

// ChunkScan represents the scan class for the chunk operator.
type ChunkScan struct {
	buffs       []*record.RecordPage
	tx          *tx.Transaction
	filename    string
	layout      *record.Layout
	startbnum   int
	endbnum     int
	currentbnum int
	rp          *record.RecordPage
	currentslot int
}

// NewChunkScan creates a chunk consisting of the specified pages.
func NewChunkScan(tx *tx.Transaction, filename string, layout *record.Layout, startbnum int, endbnum int) *ChunkScan {
	cs := &ChunkScan{
		tx:        tx,
		filename:  filename,
		layout:    layout,
		startbnum: startbnum,
		endbnum:   endbnum,
	}
	for i := startbnum; i <= endbnum; i++ {
		blk := file.NewBlockID(filename, i)
		cs.buffs = append(cs.buffs, record.NewRecordPage(tx, blk, layout))
	}
	cs.moveToBlock(startbnum)
	return cs
}

// Close closes the ChunkScan.
func (cs *ChunkScan) Close() {
	for i := 0; i < len(cs.buffs); i++ {
		blk := file.NewBlockID(cs.filename, cs.startbnum+i)
		cs.tx.Unpin(blk)
	}
}

// BeforeFirst positions the scan before the first record.
func (cs *ChunkScan) BeforeFirst() {
	cs.moveToBlock(cs.startbnum)
}

// Next moves to the next record in the current block of the chunk.
func (cs *ChunkScan) Next() bool {
	cs.currentslot = cs.rp.NextAfter(cs.currentslot)
	for cs.currentslot < 0 {
		if cs.currentbnum == cs.endbnum {
			return false
		}
		cs.moveToBlock(cs.rp.Block().Number() + 1)
		cs.currentslot = cs.rp.NextAfter(cs.currentslot)
	}
	return true
}

// GetInt retrieves the integer value of the specified field.
func (cs *ChunkScan) GetInt(fldname string) int {
	return cs.rp.GetInt(cs.currentslot, fldname)
}

// GetString retrieves the string value of the specified field.
func (cs *ChunkScan) GetString(fldname string) string {
	return cs.rp.GetString(cs.currentslot, fldname)
}

// GetVal retrieves the value of the specified field.
func (cs *ChunkScan) GetVal(fldname string) *record.Constant {
	schType, err := cs.layout.Schema().Type(fldname)
	if err != nil {
		panic(err)
	}
	if schType == record.Integer {
		return record.NewIntConstant(cs.GetInt(fldname))
	}
	return record.NewStringConstant(cs.GetString(fldname))
}

// HasField checks if the field exists in the schema.
func (cs *ChunkScan) HasField(fldname string) bool {
	return cs.layout.Schema().HasField(fldname)
}

func (cs *ChunkScan) moveToBlock(blknum int) {
	cs.currentbnum = blknum
	cs.rp = cs.buffs[cs.currentbnum-cs.startbnum]
	cs.currentslot = -1
}
