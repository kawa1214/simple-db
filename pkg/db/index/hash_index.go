package index

import (
	"github.com/kawa1214/simple-db/pkg/db/metadata"
	"github.com/kawa1214/simple-db/pkg/db/record"
	"github.com/kawa1214/simple-db/pkg/db/tx"
)

const NUM_BUCKETS = 100

type HashIndex struct {
	tx        *tx.Transaction
	idxname   string
	layout    *record.Layout
	searchkey *record.Constant
	ts        *record.TableScan
}

func NewHashIndex(tx *tx.Transaction, idxname string, layout *record.Layout) *HashIndex {
	return &HashIndex{
		tx:      tx,
		idxname: idxname,
		layout:  layout,
	}
}

func NewHashIndexFromMetadata(ii *metadata.IndexInfo) *HashIndex {
	return NewHashIndex(ii.IndexTx(), ii.IndexName(), ii.IdxLayout())
}

func (idx *HashIndex) BeforeFirst(searchkey *record.Constant) {
	idx.Close()
	idx.searchkey = searchkey
	bucket := searchkey.HashCode() % NUM_BUCKETS
	tblname := idx.idxname + string(rune(bucket))
	idx.ts = record.NewTableScan(idx.tx, tblname, idx.layout)
}

func (idx *HashIndex) Next() bool {
	for idx.ts.Next() {
		if idx.ts.GetVal("dataval").Equals(idx.searchkey) {
			return true
		}
	}
	return false
}

func (idx *HashIndex) GetDataRid() *record.RID {
	blknum := idx.ts.GetInt("block")
	id := idx.ts.GetInt("id")
	return record.NewRID(blknum, id)
}

func (idx *HashIndex) Insert(val *record.Constant, rid *record.RID) {
	idx.BeforeFirst(val)
	idx.ts.Insert()
	idx.ts.SetInt("block", rid.BlockNumber())
	idx.ts.SetInt("id", rid.Slot())
	idx.ts.SetVal("dataval", val)
}

func (idx *HashIndex) Delete(val *record.Constant, rid *record.RID) {
	idx.BeforeFirst(val)
	for idx.Next() {
		if idx.GetDataRid().Equals(rid) {
			idx.ts.Delete()
			return
		}
	}
}

func (idx *HashIndex) Close() {
	if idx.ts != nil {
		idx.ts.Close()
	}
}
