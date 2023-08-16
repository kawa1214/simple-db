package file

import (
	"fmt"
)

// BlockID represents a block identifier
type BlockID struct {
	filename string
	blknum   int
}

func NewBlockID(filename string, blknum int) *BlockID {
	return &BlockID{
		filename: filename,
		blknum:   blknum,
	}
}

func (b *BlockID) FileName() string {
	return b.filename
}

// Returns the block number, which is the identifier of the block.
func (b *BlockID) Number() int {
	return b.blknum
}

func (b *BlockID) Equal(other *BlockID) bool {
	return b.filename == other.filename && b.blknum == other.blknum
}

func (b *BlockID) String() string {
	return fmt.Sprintf("[file %s, block %d]", b.filename, b.blknum)
}
