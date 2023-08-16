package tx

import (
	"strconv"

	"github.com/kawa1214/simple-db/pkg/db/file"
	"github.com/kawa1214/simple-db/pkg/db/log"
)

// SetStringRecord represents the SETSTRING log record.
type SetStringRecord struct {
	txnum, offset int
	val           string
	blk           *file.BlockID
}

// NewSetStringRecord initializes a new SetStringRecord from a Page.
func NewSetStringRecord(p *file.Page) *SetStringRecord {
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
	val := p.GetString(vpos)
	return &SetStringRecord{txnum, offset, val, blk}
}

// Op returns the operation type of the log record.
func (sr *SetStringRecord) Op() int {
	return SETSTRING
}

// TxNumber returns the transaction number associated with the log record.
func (sr *SetStringRecord) TxNumber() int {
	return sr.txnum
}

// String returns a string representation of the log record.
func (sr *SetStringRecord) String() string {
	return "<SETSTRING " + strconv.Itoa(sr.txnum) + " " + sr.blk.String() + " " + strconv.Itoa(sr.offset) + " " + sr.val + ">"
}

// Undo replaces the specified data value with the value saved in the log record.
func (sr *SetStringRecord) Undo(tx *Transaction) {
	tx.Pin(sr.blk)
	tx.SetString(sr.blk, sr.offset, sr.val, false) // don't log the undo!
	tx.Unpin(sr.blk)
}

// WriteToLog writes a setString record to the log.
func SetStringRecordWriteToLog(lm *log.LogMgr, txnum int, blk *file.BlockID, offset int, val string) int {
	tpos := 4
	fpos := tpos + 4
	bpos := fpos + file.MaxLength(len(blk.FileName()))
	opos := bpos + 4
	vpos := opos + 4
	reclen := vpos + file.MaxLength(len(val))
	rec := make([]byte, reclen)
	p := file.NewLogPage(rec)
	p.SetInt(0, SETSTRING)
	p.SetInt(tpos, txnum)
	p.SetString(fpos, blk.FileName())
	p.SetInt(bpos, blk.Number())
	p.SetInt(opos, offset)
	p.SetString(vpos, val)
	return lm.Append(rec)
}
