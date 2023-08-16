package buffer

import (
	"errors"
	"sync"
	"time"

	"github.com/kawa1214/simple-db/pkg/db/file"
	"github.com/kawa1214/simple-db/pkg/db/log"
)

type BufferMgr struct {
	bufferpool   []*Buffer
	numAvailable int
	mu           sync.Mutex
}

const MAX_TIME = 10 * time.Second

func NewBufferMgr(fm *file.FileMgr, lm *log.LogMgr, numbuffs int) *BufferMgr {
	bm := &BufferMgr{
		bufferpool:   make([]*Buffer, numbuffs),
		numAvailable: numbuffs,
	}
	for i := 0; i < numbuffs; i++ {
		bm.bufferpool[i] = NewBuffer(fm, lm, fm.BlockSize())
	}
	return bm
}

func (bm *BufferMgr) Available() int {
	bm.mu.Lock()
	defer bm.mu.Unlock()
	return bm.numAvailable
}

func (bm *BufferMgr) FlushAll(txnum int) {
	bm.mu.Lock()
	defer bm.mu.Unlock()
	for _, buff := range bm.bufferpool {
		if buff.ModifyingTx() == txnum {
			buff.Flush()
		}
	}
}

func (bm *BufferMgr) Unpin(buff *Buffer) {
	bm.mu.Lock()
	defer bm.mu.Unlock()
	buff.Unpin()
	if !buff.IsPinned() {
		bm.numAvailable++
		return
	}
}

func (bm *BufferMgr) Pin(blk *file.BlockID) (*Buffer, error) {
	bm.mu.Lock()
	defer bm.mu.Unlock()
	startTime := time.Now()
	buff, err := bm.tryToPin(blk)
	for buff == nil && !bm.waitingTooLong(startTime) && err == nil {
		bm.mu.Unlock()
		time.Sleep(MAX_TIME)
		bm.mu.Lock()
		buff, err = bm.tryToPin(blk)
	}
	if buff == nil {
		return nil, errors.New("BufferAbortException")
	}
	return buff, nil
}

func (bm *BufferMgr) waitingTooLong(startTime time.Time) bool {
	return time.Since(startTime) > MAX_TIME
}

func (bm *BufferMgr) tryToPin(blk *file.BlockID) (*Buffer, error) {
	buff := bm.findExistingBuffer(blk)
	if buff == nil {
		buff = bm.chooseUnpinnedBuffer()
		if buff == nil {
			return nil, nil
		}
		buff.AssignToBlock(blk)
	}
	if !buff.IsPinned() {
		bm.numAvailable--
	}
	buff.Pin()
	return buff, nil
}

func (bm *BufferMgr) findExistingBuffer(blk *file.BlockID) *Buffer {
	for _, buff := range bm.bufferpool {
		b := buff.Block()
		if b != nil && b.Equal(blk) {
			return buff
		}
	}
	return nil
}

func (bm *BufferMgr) chooseUnpinnedBuffer() *Buffer {
	for _, buff := range bm.bufferpool {
		if !buff.IsPinned() {
			return buff
		}
	}
	return nil
}
