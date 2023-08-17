package tx

import (
	"github.com/kawa1214/simple-db/pkg/db/file"
	"github.com/kawa1214/simple-db/pkg/db/log"
)

// CheckpointRecord represents the CHECKPOINT log record.
type CheckpointRecord struct {
}

// NewCheckpointRecord initializes a new CheckpointRecord.
func NewCheckpointRecord() *CheckpointRecord {
	return &CheckpointRecord{}
}

// Op returns the operation type of the log record.
func (cr *CheckpointRecord) Op() int {
	return CHECKPOINT
}

// TxNumber returns the transaction number associated with the log record.
func (cr *CheckpointRecord) TxNumber() int {
	// dummy value
	return -1
}

// Undo does nothing, because a checkpoint record contains no undo information.
func (cr *CheckpointRecord) Undo(tx *Transaction) {
	// No operation required.
}

// String returns a string representation of the log record.
func (cr *CheckpointRecord) String() string {
	return "<CHECKPOINT>"
}

// WriteToLog writes a checkpoint record to the log.
// This log record contains the CHECKPOINT operator, and nothing else.
func CheckpointRecordWriteToLog(lm *log.LogMgr) int {
	rec := make([]byte, 4) // 4 bytes for an integer in Go
	p := file.NewPageFromBytes(rec)
	p.SetInt(0, CHECKPOINT)
	return lm.Append(rec)
}
