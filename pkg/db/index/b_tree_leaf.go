package index

import (
	"github.com/kawa1214/simple-db/pkg/db/file"
	"github.com/kawa1214/simple-db/pkg/db/record"
	"github.com/kawa1214/simple-db/pkg/db/tx"
)

type BTreeLeaf struct {
	tx          *tx.Transaction
	layout      *record.Layout
	searchkey   *record.Constant
	contents    *BTPage
	currentslot int
	filename    string
}

func NewBTreeLeaf(tx *tx.Transaction, blk *file.BlockID, layout *record.Layout, searchkey *record.Constant) *BTreeLeaf {
	contents := NewBTPage(tx, blk, layout)
	return &BTreeLeaf{
		tx:          tx,
		layout:      layout,
		searchkey:   searchkey,
		contents:    contents,
		currentslot: contents.FindSlotBefore(searchkey),
		filename:    blk.FileName(),
	}
}

func (leaf *BTreeLeaf) Close() {
	leaf.contents.Close()
}

func (leaf *BTreeLeaf) Next() bool {
	leaf.currentslot++
	if leaf.currentslot >= leaf.contents.GetNumRecs() {
		return leaf.tryOverflow()
	} else if leaf.contents.GetDataVal(leaf.currentslot).Equals(leaf.searchkey) {
		return true
	} else {
		return leaf.tryOverflow()
	}
}

func (leaf *BTreeLeaf) GetDataRid() *record.RID {
	return leaf.contents.GetDataRid(leaf.currentslot)
}

func (leaf *BTreeLeaf) Delete(datarid *record.RID) {
	for leaf.Next() {
		if leaf.GetDataRid().Equals(datarid) {
			leaf.contents.Delete(leaf.currentslot)
			return
		}
	}
}

func (leaf *BTreeLeaf) Insert(datarid *record.RID) (*DirEntry, error) {
	currentslot := 0
	if leaf.contents.GetFlag() >= 0 && leaf.contents.GetDataVal(0).CompareTo(leaf.searchkey) > 0 {
		firstval := leaf.contents.GetDataVal(0)
		newblk := leaf.contents.Split(0, leaf.contents.GetFlag())
		currentslot = 0
		leaf.contents.SetFlag(-1)
		leaf.contents.InsertLeaf(currentslot, leaf.searchkey, datarid)
		return &DirEntry{firstval, newblk.Number()}, nil
	}

	currentslot++
	leaf.contents.InsertLeaf(currentslot, leaf.searchkey, datarid)
	if !leaf.contents.IsFull() {
		return nil, nil
	}

	firstkey := leaf.contents.GetDataVal(0)
	lastkey := leaf.contents.GetDataVal(leaf.contents.GetNumRecs() - 1)
	if lastkey.Equals(firstkey) {
		newblk := leaf.contents.Split(1, leaf.contents.GetFlag())
		leaf.contents.SetFlag(newblk.Number())
		return nil, nil
	} else {
		splitpos := leaf.contents.GetNumRecs() / 2
		splitkey := leaf.contents.GetDataVal(splitpos)
		if splitkey.Equals(firstkey) {
			for leaf.contents.GetDataVal(splitpos).Equals(splitkey) {
				splitpos++
			}
			splitkey = leaf.contents.GetDataVal(splitpos)
		} else {
			for leaf.contents.GetDataVal(splitpos - 1).Equals(splitkey) {
				splitpos--
			}
		}
		newblk := leaf.contents.Split(splitpos, -1)
		return &DirEntry{splitkey, newblk.Number()}, nil
	}
}

func (leaf *BTreeLeaf) tryOverflow() bool {
	firstkey := leaf.contents.GetDataVal(0)
	flag := leaf.contents.GetFlag()
	if !leaf.searchkey.Equals(firstkey) || flag < 0 {
		return false
	}
	leaf.contents.Close()
	nextblk := file.NewBlockID(leaf.filename, flag)
	leaf.contents = NewBTPage(leaf.tx, nextblk, leaf.layout)
	leaf.currentslot = 0
	return true
}
