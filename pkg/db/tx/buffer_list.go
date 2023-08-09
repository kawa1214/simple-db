package tx

import (
	"github.com/kawa1214/simple-db/pkg/db/buffer"
	"github.com/kawa1214/simple-db/pkg/db/file"
)

type BufferList struct {
	buffers map[file.BlockId]*buffer.Buffer
	pins    []file.BlockId
	bm      *buffer.BufferMgr
}

// NewBufferList creates a new buffer list associated with the given buffer manager.
func NewBufferList(bm *buffer.BufferMgr) *BufferList {
	return &BufferList{
		buffers: make(map[file.BlockId]*buffer.Buffer),
		pins:    make([]file.BlockId, 0),
		bm:      bm,
	}
}

// GetBuffer returns the buffer pinned to the specified block.
// The method returns nil if the transaction has not pinned the block.
func (bl *BufferList) GetBuffer(blk file.BlockId) *buffer.Buffer {
	return bl.buffers[blk]
}

// Pin pins the block and keeps track of the buffer internally.
func (bl *BufferList) Pin(blk *file.BlockId) {
	buff, err := bl.bm.Pin(blk)
	if err != nil {
		panic(err)
	}
	bl.buffers[*blk] = buff
	bl.pins = append(bl.pins, *blk)
}

// Unpin unpins the specified block.
func (bl *BufferList) Unpin(blk file.BlockId) {
	buff := bl.buffers[blk]
	bl.bm.Unpin(buff)
	bl.removeBlockFromPins(blk)
	if !bl.containsBlockInPins(blk) {
		delete(bl.buffers, blk)
	}
}

// UnpinAll unpins any buffers still pinned by this transaction.
func (bl *BufferList) UnpinAll() {
	for _, blk := range bl.pins {
		buff := bl.buffers[blk]
		bl.bm.Unpin(buff)
	}
	bl.buffers = make(map[file.BlockId]*buffer.Buffer)
	bl.pins = make([]file.BlockId, 0)
}

// containsBlockInPins checks if the pins slice contains the specified block.
func (bl *BufferList) containsBlockInPins(blk file.BlockId) bool {
	for _, b := range bl.pins {
		if b == blk {
			return true
		}
	}
	return false
}

// removeBlockFromPins removes the first occurrence of the specified block from the pins slice.
func (bl *BufferList) removeBlockFromPins(blk file.BlockId) {
	for i, b := range bl.pins {
		if b == blk {
			bl.pins = append(bl.pins[:i], bl.pins[i+1:]...)
			break
		}
	}
}
