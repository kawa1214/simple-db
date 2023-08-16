package tx

import (
	"sync"
	"time"

	"github.com/kawa1214/simple-db/pkg/db/file"
)

const maxTime = 10 * time.Second

// LockAbortException represents a lock timeout.
type LockAbortException struct{}

func (e LockAbortException) Error() string {
	return "Lock request aborted due to timeout"
}

// LockTable provides methods to lock and unlock blocks.
type LockTable struct {
	locks map[file.BlockID]int
	mu    sync.Mutex
	cond  *sync.Cond
}

// NewLockTable creates a new lock table.
func NewLockTable() *LockTable {
	lt := &LockTable{
		locks: make(map[file.BlockID]int),
	}
	lt.cond = sync.NewCond(&lt.mu)
	return lt
}

// SLock grants an SLock on the specified block.
func (lt *LockTable) SLock(blk file.BlockID) {
	lt.mu.Lock()
	defer lt.mu.Unlock()

	timestamp := time.Now()
	for lt.hasXlock(blk) && !waitingTooLong(timestamp) {
		lt.cond.Wait()
	}
	if lt.hasXlock(blk) {
		panic(LockAbortException{})
	}
	val := lt.getLockVal(blk) // will not be negative
	lt.locks[blk] = val + 1
}

// XLock grants an XLock on the specified block.
func (lt *LockTable) XLock(blk file.BlockID) {
	lt.mu.Lock()
	defer lt.mu.Unlock()

	timestamp := time.Now()
	for lt.hasOtherSLocks(blk) && !waitingTooLong(timestamp) {
		lt.cond.Wait()
	}
	if lt.hasOtherSLocks(blk) {
		panic(LockAbortException{})
	}
	lt.locks[blk] = -1
}

// Unlock releases a lock on the specified block.
func (lt *LockTable) Unlock(blk file.BlockID) {
	lt.mu.Lock()
	defer lt.mu.Unlock()

	val := lt.getLockVal(blk)
	if val > 1 {
		lt.locks[blk] = val - 1
	} else {
		delete(lt.locks, blk)
		lt.cond.Broadcast()
	}
}

func (lt *LockTable) hasXlock(blk file.BlockID) bool {
	return lt.getLockVal(blk) < 0
}

func (lt *LockTable) hasOtherSLocks(blk file.BlockID) bool {
	return lt.getLockVal(blk) > 1
}

func waitingTooLong(startTime time.Time) bool {
	return time.Since(startTime) > maxTime
}

func (lt *LockTable) getLockVal(blk file.BlockID) int {
	val, ok := lt.locks[blk]
	if !ok {
		return 0
	}
	return val
}
