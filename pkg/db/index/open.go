package index

import "github.com/kawa1214/simple-db/pkg/db/metadata"

// Open opens the index described by this object.
// → NewHashIndexFromMetadata
// func (ii *IndexInfo) Open() Index {
// 	return NewHashIndex(ii.tx, ii.idxname, ii.idxLayout)
// 	// return NewBTreeIndex(ii.tx, ii.idxname, ii.idxLayout)
// }

// TODO:
// BlocksAccessed estimates the number of block accesses required to find all index records.
// func (ii *IndexInfo) BlocksAccessed() int {
// 	rpb := ii.tx.BlockSize() / ii.idxLayout.SlotSize()
// 	numblocks := ii.si.RecordsOutput() / rpb
// 	return HashIndexSearchCost(numblocks, rpb)
// 	// return BTreeIndexSearchCost(numblocks, rpb)
// }

func Open(ii *metadata.IndexInfo) Index {
	return NewHashIndexFromMetadata(ii)
	// return NewBTreeIndexFromMetadata(ii)
}

func BlocksAccessed(ii *metadata.IndexInfo) int {
	rpb := ii.IndexTx().BlockSize() / ii.IdxLayout().SlotSize()
	numblocks := ii.Si().RecordsOutput() / rpb
	return HashIndexSearchCost(numblocks, rpb)
	// return BTreeIndexSearchCost(numblocks, rpb)
}

func HashIndexSearchCost(numblocks int, rpb int) int {
	return numblocks / NUM_BUCKETS
}
