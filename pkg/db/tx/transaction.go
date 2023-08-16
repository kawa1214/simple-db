package tx

import (
	"sync"
	"sync/atomic"

	"github.com/kawa1214/simple-db/pkg/db/buffer"
	"github.com/kawa1214/simple-db/pkg/db/file"
	"github.com/kawa1214/simple-db/pkg/db/log"
)

type Transaction struct {
	recoveryMgr *RecoveryMgr
	concurMgr   *ConcurrencyMgr
	bm          *buffer.BufferMgr
	fm          *file.FileMgr
	txnum       int
	mybuffers   *BufferList
}

var nextTxNum int32 = 0

const END_OF_FILE = -1

func NewTransaction(fm *file.FileMgr, lm *log.LogMgr, bm *buffer.BufferMgr) *Transaction {
	txnum := nextTxNumber()
	cm := NewConcurrencyMgr()
	rm := NewRecoveryMgr(nil, txnum, lm, bm)
	tx := &Transaction{
		fm:          fm,
		bm:          bm,
		txnum:       txnum,
		recoveryMgr: rm,
		concurMgr:   cm,
		mybuffers:   NewBufferList(bm),
	}
	rm.tx = tx
	return tx
}

func (t *Transaction) Commit() {
	t.recoveryMgr.Commit()
	// fmt.Println("transaction", t.txnum, "committed")
	t.concurMgr.Release()
	t.mybuffers.UnpinAll()
}

func (t *Transaction) Rollback() {
	t.recoveryMgr.Rollback()
	// fmt.Println("transaction", t.txnum, "rolled back")
	t.concurMgr.Release()
	t.mybuffers.UnpinAll()
}

func (t *Transaction) Recover() {
	t.bm.FlushAll(t.txnum)
	t.recoveryMgr.Recover()
}

func (t *Transaction) Pin(blk *file.BlockID) {
	t.mybuffers.Pin(blk)
}

func (t *Transaction) Unpin(blk *file.BlockID) {
	t.mybuffers.Unpin(*blk)
}

func (t *Transaction) GetInt(blk *file.BlockID, offset int) int {
	t.concurMgr.SLock(*blk)
	buff := t.mybuffers.GetBuffer(*blk)
	return buff.Contents().GetInt(offset)
}

func (t *Transaction) GetString(blk *file.BlockID, offset int) string {
	t.concurMgr.SLock(*blk)
	buff := t.mybuffers.GetBuffer(*blk)
	return buff.Contents().GetString(offset)
}

func (t *Transaction) SetInt(blk *file.BlockID, offset int, val int, okToLog bool) {
	t.concurMgr.XLock(*blk)
	buff := t.mybuffers.GetBuffer(*blk)
	var lsn int = -1
	if okToLog {
		var err error
		lsn, err = t.recoveryMgr.SetInt(buff, offset, val)
		if err != nil {
			panic(err)
		}
	}
	p := buff.Contents()
	p.SetInt(offset, val)
	buff.SetModified(t.txnum, lsn)
}

func (t *Transaction) SetString(blk *file.BlockID, offset int, val string, okToLog bool) {
	t.concurMgr.XLock(*blk)
	buff := t.mybuffers.GetBuffer(*blk)
	var lsn int = -1
	if okToLog {
		var err error
		lsn, err = t.recoveryMgr.SetString(buff, offset, val)
		if err != nil {
			panic(err)
		}
	}
	p := buff.Contents()
	p.SetString(offset, val)
	buff.SetModified(t.txnum, lsn)
}

func (t *Transaction) Size(filename string) int {
	dummyblk := file.NewBlockID(filename, END_OF_FILE)
	t.concurMgr.SLock(*dummyblk)
	len, err := t.fm.Length(filename)
	if err != nil {
		panic(err)
	}
	return len
}

func (t *Transaction) Append(filename string) *file.BlockID {
	dummyblk := file.NewBlockID(filename, END_OF_FILE)
	t.concurMgr.XLock(*dummyblk)
	block, err := t.fm.Append(filename)
	if err != nil {
		panic(err)
	}
	return block
}

func (t *Transaction) BlockSize() int {
	return t.fm.BlockSize()
}

func (t *Transaction) AvailableBuffs() int {
	return t.bm.Available()
}

var mu sync.Mutex

func nextTxNumber() int {
	mu.Lock()
	defer mu.Unlock()
	num := atomic.AddInt32(&nextTxNum, 1)
	return int(num)
}
