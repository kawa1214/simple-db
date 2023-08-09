package tx

import (
	"github.com/kawa1214/simple-db/pkg/db/buffer"
	"github.com/kawa1214/simple-db/pkg/db/log"
)

type RecoveryMgr struct {
	lm    *log.LogMgr
	bm    *buffer.BufferMgr
	tx    *Transaction
	txnum int
}

func NewRecoveryMgr(tx *Transaction, txnum int, lm *log.LogMgr, bm *buffer.BufferMgr) *RecoveryMgr {
	rm := &RecoveryMgr{tx: tx, txnum: txnum, lm: lm, bm: bm}
	StartRecordWriteToLog(lm, txnum)
	return rm
}

func (rm *RecoveryMgr) Commit() {
	rm.bm.FlushAll(rm.txnum)
	lsn := CommitRecordWriteToLog(rm.lm, rm.txnum)
	rm.lm.Flush(lsn)
}

func (rm *RecoveryMgr) Rollback() {
	rm.doRollback()
	rm.bm.FlushAll(rm.txnum)
	lsn := RollbackRecordWriteToLog(rm.lm, rm.txnum)
	rm.lm.Flush(lsn)
}

func (rm *RecoveryMgr) Recover() {
	rm.doRecover()
	rm.bm.FlushAll(rm.txnum)
	lsn := CheckpointRecordWriteToLog(rm.lm)
	rm.lm.Flush(lsn)
}

func (rm *RecoveryMgr) SetInt(buff *buffer.Buffer, offset int, newval int) (int, error) {
	oldval := buff.Contents().GetInt(offset)
	blk := buff.Block()
	return SetIntRecordWriteToLog(rm.lm, rm.txnum, blk, offset, oldval), nil
}

func (rm *RecoveryMgr) SetString(buff *buffer.Buffer, offset int, newval string) (int, error) {
	oldval := buff.Contents().GetString(offset)
	blk := buff.Block()
	return SetStringRecordWriteToLog(rm.lm, rm.txnum, blk, offset, oldval), nil
}

func (rm *RecoveryMgr) doRollback() {
	iter := rm.lm.Iterator()
	for iter.HasNext() {
		bytes := iter.Next()
		rec := CreateLogRecord(bytes)
		if rec.TxNumber() == rm.txnum {
			if rec.Op() == START {
				return
			}
			rec.Undo(rm.tx)
		}
	}
}

func (rm *RecoveryMgr) doRecover() {
	finishedTxs := make(map[int]bool)
	iter := rm.lm.Iterator()
	for iter.HasNext() {
		bytes := iter.Next()
		rec := CreateLogRecord(bytes)
		if rec.Op() == CHECKPOINT {
			return
		}
		if rec.Op() == COMMIT || rec.Op() == ROLLBACK {
			finishedTxs[rec.TxNumber()] = true
		} else if !finishedTxs[rec.TxNumber()] {
			rec.Undo(rm.tx)
		}
	}
}
