package index

import "github.com/kawa1214/simple-db/pkg/db/record"

// Index defines methods to traverse an index.
type Index interface {
	// BeforeFirst positions the index before the first record
	// having the specified search key.
	BeforeFirst(searchKey *record.Constant)

	// Next moves the index to the next record having the
	// search key specified in the BeforeFirst method.
	// Returns false if there are no more such index records.
	Next() bool

	// GetDataRid returns the dataRID value stored in the current index record.
	GetDataRid() *record.RID

	// Insert inserts an index record having the specified
	// dataval and dataRID values.
	Insert(dataval *record.Constant, datarid *record.RID)

	// Delete deletes the index record having the specified
	// dataval and dataRID values.
	Delete(dataval *record.Constant, datarid *record.RID)

	// Close closes the index.
	Close()
}
