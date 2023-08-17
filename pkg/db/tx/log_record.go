package tx

import "github.com/kawa1214/simple-db/pkg/db/file"

type LogRecord interface {
	Op() int
	TxNumber() int
	Undo(tx *Transaction)
}

const (
	CHECKPOINT = iota
	START
	COMMIT
	ROLLBACK
	SETINT
	SETSTRING
)

func CreateLogRecord(bytes []byte) LogRecord {
	p := file.NewPageFromBytes(bytes) // Assuming you have a corresponding NewPage function
	switch p.GetInt(0) {
	case CHECKPOINT:
		return NewCheckpointRecord()
	case START:
		return NewStartRecord(p) // Assuming constructor-like functions
	case COMMIT:
		return NewCommitRecord(p)
	case ROLLBACK:
		return NewRollbackRecord(p)
	case SETINT:
		return NewSetIntRecord(p)
	case SETSTRING:
		return NewSetStringRecord(p)
	default:
		return nil
	}
}
