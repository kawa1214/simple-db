package index

import (
	"math"

	"github.com/kawa1214/simple-db/pkg/db/file"
	"github.com/kawa1214/simple-db/pkg/db/record"
	"github.com/kawa1214/simple-db/pkg/db/tx"
)

type BTreeIndex struct {
	tx         *tx.Transaction
	dirLayout  *record.Layout
	leafLayout *record.Layout
	leaftbl    string
	leaf       *BTreeLeaf
	rootblk    *file.BlockId
}

func NewBTreeIndex(tx *tx.Transaction, idxname string, leafLayout *record.Layout) *BTreeIndex {
	// deal with the leaves
	leaftbl := idxname + "leaf"
	if tx.Size(leaftbl) == 0 {
		blk := tx.Append(leaftbl)
		node := NewBTPage(tx, blk, leafLayout)
		node.Format(blk, -1)
	}

	// deal with the directory
	dirsch := record.NewSchema()
	dirsch.Add("block", leafLayout.Schema())
	dirsch.Add("dataval", leafLayout.Schema())
	dirLayout := record.NewLayoutFromSchema(dirsch)
	dirtbl := idxname + "dir"
	rootblk := file.NewBlockId(dirtbl, 0)
	if tx.Size(dirtbl) == 0 {
		tx.Append(dirtbl)
		node := NewBTPage(tx, rootblk, dirLayout)
		node.Format(rootblk, 0)
		// insert initial directory entry
		fldType, err := dirsch.Type("dataval")
		if err != nil {
			panic(err)
		}

		var minval *record.Constant
		if fldType == record.Integer {
			minval = record.NewIntConstant(math.MinInt32)
		} else {
			minval = record.NewStringConstant("")
		}

		node.InsertDir(0, minval, 0)
		node.Close()
	}

	return &BTreeIndex{
		tx:         tx,
		dirLayout:  dirLayout,
		leafLayout: leafLayout,
		leaftbl:    leaftbl,
		rootblk:    rootblk,
	}
}

func (idx *BTreeIndex) BeforeFirst(searchkey *record.Constant) {
	idx.Close()
	root := NewBTreeDir(idx.tx, idx.rootblk, idx.dirLayout)
	blknum := root.Search(searchkey)
	root.Close()
	leafblk := file.NewBlockId(idx.leaftbl, blknum)
	idx.leaf = NewBTreeLeaf(idx.tx, leafblk, idx.leafLayout, searchkey)
}

func (idx *BTreeIndex) Next() bool {
	return idx.leaf.Next()
}

func (idx *BTreeIndex) GetDataRid() *record.RID {
	return idx.leaf.GetDataRid()
}

func (idx *BTreeIndex) Insert(dataval *record.Constant, datarid *record.RID) {
	idx.BeforeFirst(dataval)
	e, err := idx.leaf.Insert(datarid)
	if err != nil {
		panic(err)
	}
	idx.leaf.Close()
	if e == nil {
		return
	}
	root := NewBTreeDir(idx.tx, idx.rootblk, idx.dirLayout)
	e2 := root.Insert(e)
	if e2 != nil {
		root.MakeNewRoot(e2)
	}
	root.Close()
}

func (idx *BTreeIndex) Delete(dataval *record.Constant, datarid *record.RID) {
	idx.BeforeFirst(dataval)
	idx.leaf.Delete(datarid)
	idx.leaf.Close()
}

func (idx *BTreeIndex) Close() {
	if idx.leaf != nil {
		idx.leaf.Close()
	}
}

func BtreeIndexSearchCost(numblocks int, rpb int) int {
	return 1 + int(math.Log(float64(numblocks))/math.Log(float64(rpb)))
}
