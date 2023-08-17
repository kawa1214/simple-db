package tx

import (
	"strconv"

	"github.com/kawa1214/simple-db/pkg/db/file"
	"github.com/kawa1214/simple-db/pkg/db/log"
)

type RollbackRecord struct {
	txnum int
}

// NewRollbackRecord initializes a new RollbackRecord from a Page.
func NewRollbackRecord(p *file.Page) *RollbackRecord {
	tpos := 4 // 4 bytes for an integer in Go
	return &RollbackRecord{
		txnum: p.GetInt(tpos),
	}
}

// Op returns the operation type of the log record.
func (rr *RollbackRecord) Op() int {
	return ROLLBACK
}

// TxNumber returns the transaction number associated with the log record.
func (rr *RollbackRecord) TxNumber() int {
	return rr.txnum
}

// Undo does nothing, because a rollback record contains no undo information.
func (rr *RollbackRecord) Undo(tx *Transaction) {
	// No operation required.
}

// String returns a string representation of the log record.
func (rr *RollbackRecord) String() string {
	return "<ROLLBACK " + strconv.Itoa(rr.txnum) + ">"
}

// WriteToLog writes a rollback record to the log.
// This log record contains the ROLLBACK operator, followed by the transaction id.
func RollbackRecordWriteToLog(lm *log.LogMgr, txnum int) int {
	rec := make([]byte, 8) // 8 bytes for two integers in Go
	p := file.NewPageFromBytes(rec)
	p.SetInt(0, ROLLBACK)
	p.SetInt(4, txnum) // set at position 4 as integer occupies 4 bytes
	return lm.Append(rec)
}
