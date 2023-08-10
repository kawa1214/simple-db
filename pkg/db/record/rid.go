package record

import "fmt"

type RID struct {
	blknum int
	slot   int
}

// NewRID creates a new RID for the record having the
// specified location in the specified block.
func NewRID(blknum, slot int) *RID {
	return &RID{blknum: blknum, slot: slot}
}

// BlockNumber returns the block number associated with this RID.
func (rid *RID) BlockNumber() int {
	return rid.blknum
}

// Slot returns the slot associated with this RID.
func (rid *RID) Slot() int {
	return rid.slot
}

// Equals compares this RID with another RID for equality.
func (rid *RID) Equals(other *RID) bool {
	return rid.blknum == other.blknum && rid.slot == other.slot
}

// String returns the string representation of the RID.
func (rid *RID) String() string {
	return "[" + fmt.Sprintf("%d", rid.blknum) + ", " + fmt.Sprintf("%d", rid.slot) + "]"
}
