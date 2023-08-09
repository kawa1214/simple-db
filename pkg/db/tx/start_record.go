package tx

import (
	"strconv"

	"github.com/kawa1214/simple-db/pkg/db/file"
	"github.com/kawa1214/simple-db/pkg/db/log"
)

type StartRecord struct {
	txnum int
}

// NewStartRecord initializes a new StartRecord from a Page.
func NewStartRecord(p *file.Page) *StartRecord {
	tpos := 4 // 4 bytes for an integer in Go
	txnum := p.GetInt(tpos)
	return &StartRecord{txnum}
}

// Op returns the operation type of the log record.
func (sr *StartRecord) Op() int {
	return START
}

// TxNumber returns the transaction number associated with the log record.
func (sr *StartRecord) TxNumber() int {
	return sr.txnum
}

// Undo does nothing since a start record contains no undo information.
func (sr *StartRecord) Undo(tx *Transaction) {}

// String returns a string representation of the log record.
func (sr *StartRecord) String() string {
	return "<START " + strconv.Itoa(sr.txnum) + ">"
}

// WriteToLog writes a start record to the log.
func StartRecordWriteToLog(lm *log.LogMgr, txnum int) int {
	rec := make([]byte, 2*4)
	p := file.NewLogPage(rec)
	p.SetInt(0, START)
	p.SetInt(4, txnum)
	return lm.Append(rec)
}
