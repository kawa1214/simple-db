package tx

import (
	"strconv"

	"github.com/kawa1214/simple-db/pkg/db/file"
	"github.com/kawa1214/simple-db/pkg/db/log"
)

type SetIntRecord struct {
	txnum, offset, val int
	blk                *file.BlockID
}

// NewSetIntRecord initializes a new SetIntRecord from a Page.
func NewSetIntRecord(p *file.Page) *SetIntRecord {
	tpos := 4 // 4 bytes for an integer in Go
	txnum := p.GetInt(tpos)
	fpos := tpos + 4
	filename := p.GetString(fpos)
	bpos := fpos + file.MaxLength(len(filename))
	blknum := p.GetInt(bpos)
	blk := file.NewBlockID(filename, blknum)
	opos := bpos + 4
	offset := p.GetInt(opos)
	vpos := opos + 4
	val := p.GetInt(vpos)
	return &SetIntRecord{txnum, offset, val, blk}
}

// Op returns the operation type of the log record.
func (sr *SetIntRecord) Op() int {
	return SETINT
}

// TxNumber returns the transaction number associated with the log record.
func (sr *SetIntRecord) TxNumber() int {
	return sr.txnum
}

// String returns a string representation of the log record.
func (sr *SetIntRecord) String() string {
	return "<SETINT " + strconv.Itoa(sr.txnum) + " " + sr.blk.String() + " " + strconv.Itoa(sr.offset) + " " + strconv.Itoa(sr.val) + ">"
}

// Undo replaces the specified data value with the value saved in the log record.
func (sr *SetIntRecord) Undo(tx *Transaction) {
	tx.Pin(sr.blk)
	tx.SetInt(sr.blk, sr.offset, sr.val, false) // don't log the undo!
	tx.Unpin(sr.blk)
}

// WriteToLog writes a setInt record to the log.
// This log record contains the SETINT operator, followed by the transaction id, the filename, number, and offset of the modified block, and the previous integer value at that offset.
func SetIntRecordWriteToLog(lm *log.LogMgr, txnum int, blk *file.BlockID, offset int, val int) int {
	tpos := 4
	fpos := tpos + 4
	bpos := fpos + file.MaxLength(len(blk.FileName()))
	opos := bpos + 4
	vpos := opos + 4
	rec := make([]byte, vpos+4)
	p := file.NewLogPage(rec)
	p.SetInt(0, SETINT)
	p.SetInt(tpos, txnum)
	p.SetString(fpos, blk.FileName())
	p.SetInt(bpos, blk.Number())
	p.SetInt(opos, offset)
	p.SetInt(vpos, val)
	return lm.Append(rec)
}
