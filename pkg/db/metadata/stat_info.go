package metadata

// StatInfo holds statistical information about a table:
// the number of blocks, the number of records,
// and the number of distinct values for each field.
type StatInfo struct {
	numBlocks int
	numRecs   int
}

// NewStatInfo creates a StatInfo object.
// Note that the number of distinct values is not
// passed into the constructor.
// The function fakes this value.
func NewStatInfo(numblocks, numrecs int) *StatInfo {
	return &StatInfo{
		numBlocks: numblocks,
		numRecs:   numrecs,
	}
}

// BlocksAccessed returns the estimated number of blocks in the table.
func (si *StatInfo) BlocksAccessed() int {
	return si.numBlocks
}

// RecordsOutput returns the estimated number of records in the table.
func (si *StatInfo) RecordsOutput() int {
	return si.numRecs
}

// DistinctValues returns the estimated number of distinct values
// for the specified field.
// This estimate is a complete guess, because doing something
// reasonable is beyond the scope of this system.
func (si *StatInfo) DistinctValues(fldname string) int {
	return 1 + (si.numRecs / 3)
}
