package metadata

import (
	"github.com/kawa1214/simple-db/pkg/db/record"
	"github.com/kawa1214/simple-db/pkg/db/tx"
)

type IndexInfo struct {
	idxname   string
	fldname   string
	tx        *tx.Transaction
	tblSchema *record.Schema
	idxLayout *record.Layout
	si        *StatInfo
}

// NewIndexInfo creates an IndexInfo object for the specified index.
func NewIndexInfo(idxname, fldname string, tblSchema *record.Schema, tx *tx.Transaction, si *StatInfo) *IndexInfo {
	ii := &IndexInfo{
		idxname:   idxname,
		fldname:   fldname,
		tx:        tx,
		tblSchema: tblSchema,
		si:        si,
	}
	ii.idxLayout = ii.createIdxLayout()
	return ii
}

// Open opens the index described by this object.
// → NewHashIndexFromMetadata
// func (ii *IndexInfo) Open() Index {
// 	return NewHashIndex(ii.tx, ii.idxname, ii.idxLayout)
// 	// return NewBTreeIndex(ii.tx, ii.idxname, ii.idxLayout)
// }

func (ii *IndexInfo) IndexName() string {
	return ii.idxname
}

func (ii *IndexInfo) IdxLayout() *record.Layout {
	return ii.idxLayout
}

func (ii *IndexInfo) IndexTx() *tx.Transaction {
	return ii.tx
}

func (ii *IndexInfo) Si() *StatInfo {
	return ii.si
}

// TODO:
// BlocksAccessed estimates the number of block accesses required to find all index records.
// func (ii *IndexInfo) BlocksAccessed() int {
// 	rpb := ii.tx.BlockSize() / ii.idxLayout.SlotSize()
// 	numblocks := ii.si.RecordsOutput() / rpb
// 	return HashIndexSearchCost(numblocks, rpb)
// 	// return BTreeIndexSearchCost(numblocks, rpb)
// }

// RecordsOutput returns the estimated number of records having a search key.
func (ii *IndexInfo) RecordsOutput() int {
	return ii.si.RecordsOutput() / ii.si.DistinctValues(ii.fldname)
}

// DistinctValues returns the distinct values for a specified field or 1 for the indexed field.
func (ii *IndexInfo) DistinctValues(fname string) int {
	if ii.fldname == fname {
		return 1
	}
	return ii.si.DistinctValues(ii.fldname)
}

// createIdxLayout returns the layout of the index records.
func (ii *IndexInfo) createIdxLayout() *record.Layout {
	sch := record.NewSchema()
	sch.AddIntField("block")
	sch.AddIntField("id")
	schType, err := ii.tblSchema.Type(ii.fldname)
	if err != nil {
		panic(err)
	}
	if schType == record.Integer {
		sch.AddIntField("dataval")
	} else {
		fldlen, err := ii.tblSchema.Length(ii.fldname)
		if err != nil {
			panic(err)
		}
		sch.AddStringField("dataval", fldlen)
	}
	return record.NewLayoutFromSchema(sch)
}
