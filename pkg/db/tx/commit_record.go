package tx

import (
	"strconv"

	"github.com/kawa1214/simple-db/pkg/db/file"
	"github.com/kawa1214/simple-db/pkg/db/log"
)

type CommitRecord struct {
	txnum int
}

// NewCommitRecord initializes a new CommitRecord from a Page.
func NewCommitRecord(p *file.Page) *CommitRecord {
	tpos := 4 // 4 bytes for an integer in Go
	return &CommitRecord{
		txnum: p.GetInt(tpos),
	}
}

// Op returns the operation type of the log record.
func (cr *CommitRecord) Op() int {
	return COMMIT
}

// TxNumber returns the transaction number associated with the log record.
func (cr *CommitRecord) TxNumber() int {
	return cr.txnum
}

// Undo does nothing, because a commit record contains no undo information.
func (cr *CommitRecord) Undo(tx *Transaction) {
	// No operation required.
}

// String returns a string representation of the log record.
func (cr *CommitRecord) String() string {
	return "<COMMIT " + strconv.Itoa(cr.txnum) + ">"
}

// WriteToLog writes a commit record to the log.
// This log record contains the COMMIT operator, followed by the transaction id.
func CommitRecordWriteToLog(lm *log.LogMgr, txnum int) int {
	rec := make([]byte, 8) // 8 bytes for two integers in Go
	p := file.NewLogPage(rec)
	p.SetInt(0, COMMIT)
	p.SetInt(4, txnum) // set at position 4 as integer occupies 4 bytes
	return lm.Append(rec)
}
