package tx

import "github.com/kawa1214/simple-db/pkg/db/file"

type ConcurrencyMgr struct {
	LockTable *LockTable
	Locks     map[file.BlockId]string
}

// NewConcurrencyMgr initializes a new concurrency manager.
func NewConcurrencyMgr() *ConcurrencyMgr {
	return &ConcurrencyMgr{
		LockTable: &LockTable{},
		Locks:     make(map[file.BlockId]string),
	}
}

// SLock obtains an SLock on the block, if necessary.
func (cm *ConcurrencyMgr) SLock(blk file.BlockId) {
	if _, exists := cm.Locks[blk]; !exists {
		cm.LockTable.SLock(blk)
		cm.Locks[blk] = "S"
	}
}

// XLock obtains an XLock on the block, if necessary.
func (cm *ConcurrencyMgr) XLock(blk file.BlockId) {
	if !cm.HasXLock(blk) {
		cm.SLock(blk)
		cm.LockTable.XLock(blk)
		cm.Locks[blk] = "X"
	}
}

// Release releases all locks.
func (cm *ConcurrencyMgr) Release() {
	for blk := range cm.Locks {
		cm.LockTable.Unlock(blk)
		delete(cm.Locks, blk)
	}
}

// HasXLock checks if the block has an XLock.
func (cm *ConcurrencyMgr) HasXLock(blk file.BlockId) bool {
	lockType, exists := cm.Locks[blk]
	return exists && lockType == "X"
}
