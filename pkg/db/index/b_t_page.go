package index

import (
	"github.com/kawa1214/simple-db/pkg/db/file"
	"github.com/kawa1214/simple-db/pkg/db/record"
	"github.com/kawa1214/simple-db/pkg/db/tx"
)

const (
	intSize = 4
)

type BTPage struct {
	tx         *tx.Transaction
	currentblk *file.BlockId
	layout     *record.Layout
}

func NewBTPage(tx *tx.Transaction, currentblk *file.BlockId, layout *record.Layout) *BTPage {
	tx.Pin(currentblk)
	return &BTPage{
		tx:         tx,
		currentblk: currentblk,
		layout:     layout,
	}
}

func (p *BTPage) FindSlotBefore(searchkey *record.Constant) int {
	slot := 0
	for slot < p.GetNumRecs() && p.GetDataVal(slot).CompareTo(searchkey) < 0 {
		slot++
	}
	return slot - 1
}

func (p *BTPage) Close() {
	if p.currentblk != nil {
		p.tx.Unpin(p.currentblk)
	}
	p.currentblk = nil
}

func (p *BTPage) IsFull() bool {
	return p.slotpos(p.GetNumRecs()+1) >= p.tx.BlockSize()
}

func (p *BTPage) Split(splitpos int, flag int) *file.BlockId {
	newblk := p.appendNew(flag)
	newpage := NewBTPage(p.tx, newblk, p.layout)
	p.transferRecs(splitpos, newpage)
	newpage.SetFlag(flag)
	newpage.Close()
	return newblk
}

func (p *BTPage) GetDataVal(slot int) *record.Constant {
	return p.GetVal(slot, "dataval")
}

func (p *BTPage) GetFlag() int {
	return p.tx.GetInt(p.currentblk, 0)
}

func (p *BTPage) SetFlag(val int) {
	p.tx.SetInt(p.currentblk, 0, val, true)
}

func (p *BTPage) appendNew(flag int) *file.BlockId {
	blk := p.tx.Append(p.currentblk.FileName())

	p.tx.Pin(blk)
	p.Format(blk, flag)
	return blk
}

func (p *BTPage) Format(blk *file.BlockId, flag int) {
	p.tx.SetInt(blk, 0, flag, false)
	p.tx.SetInt(blk, intSize, 0, false)
	recsize := p.layout.SlotSize()
	for pos := 2 * intSize; pos+recsize <= p.tx.BlockSize(); pos += recsize {
		p.makeDefaultRecord(blk, pos)
	}
}

func (p *BTPage) makeDefaultRecord(blk *file.BlockId, pos int) {
	for _, fldname := range p.layout.Schema().Fields {
		offset := p.layout.Offset(fldname)
		schType, err := p.layout.Schema().Type(fldname)
		if err != nil {
			panic(err)
		}
		if schType == record.Integer {
			p.tx.SetInt(blk, pos+offset, 0, false)
		} else {
			p.tx.SetString(blk, pos+offset, "", false)
		}
	}
}

func (p *BTPage) GetChildNum(slot int) int {
	return p.GetInt(slot, "block")
}

func (p *BTPage) InsertDir(slot int, val *record.Constant, blknum int) {
	p.Insert(slot)
	p.SetVal(slot, "dataval", val)
	p.SetInt(slot, "block", blknum)
}

func (p *BTPage) GetDataRid(slot int) *record.RID {
	return record.NewRID(p.GetInt(slot, "block"), p.GetInt(slot, "id"))
}

func (p *BTPage) InsertLeaf(slot int, val *record.Constant, rid *record.RID) {
	p.Insert(slot)
	p.SetVal(slot, "dataval", val)
	p.SetInt(slot, "block", rid.BlockNumber())
	p.SetInt(slot, "id", rid.Slot())
}

func (p *BTPage) Delete(slot int) {
	for i := slot + 1; i < p.GetNumRecs(); i++ {
		p.copyRecord(i, i-1)
	}
	p.SetNumRecs(p.GetNumRecs() - 1)
}

func (p *BTPage) GetNumRecs() int {
	return p.tx.GetInt(p.currentblk, intSize)
}

func (p *BTPage) GetInt(slot int, fldname string) int {
	pos := p.fldpos(slot, fldname)
	return p.tx.GetInt(p.currentblk, pos)
}

func (p *BTPage) GetString(slot int, fldname string) string {
	pos := p.fldpos(slot, fldname)
	return p.tx.GetString(p.currentblk, pos)
}

func (p *BTPage) GetVal(slot int, fldname string) *record.Constant {
	schType, err := p.layout.Schema().Type(fldname)
	if err != nil {
		panic(err)
	}
	if schType == record.Integer {
		return record.NewIntConstant(p.GetInt(slot, fldname))
	}
	return record.NewStringConstant(p.GetString(slot, fldname))
}

func (p *BTPage) SetInt(slot int, fldname string, val int) {
	pos := p.fldpos(slot, fldname)
	p.tx.SetInt(p.currentblk, pos, val, true)
}

func (p *BTPage) SetString(slot int, fldname string, val string) {
	pos := p.fldpos(slot, fldname)
	p.tx.SetString(p.currentblk, pos, val, true)
}

func (p *BTPage) SetVal(slot int, fldname string, val *record.Constant) {
	schType, err := p.layout.Schema().Type(fldname)
	if err != nil {
		panic(err)
	}
	if schType == record.Integer {
		p.SetInt(slot, fldname, val.AsInt())
	} else {
		p.SetString(slot, fldname, val.AsString())
	}
}

func (p *BTPage) SetNumRecs(n int) {
	p.tx.SetInt(p.currentblk, intSize, n, true)
}

func (p *BTPage) Insert(slot int) {
	for i := p.GetNumRecs(); i > slot; i-- {
		p.copyRecord(i-1, i)
	}
	p.SetNumRecs(p.GetNumRecs() + 1)
}

func (p *BTPage) copyRecord(from, to int) {
	for _, fldname := range p.layout.Schema().Fields {
		p.SetVal(to, fldname, p.GetVal(from, fldname))
	}
}

func (p *BTPage) transferRecs(slot int, dest *BTPage) {
	destslot := 0
	for slot < p.GetNumRecs() {
		dest.Insert(destslot)
		for _, fldname := range p.layout.Schema().Fields {
			dest.SetVal(destslot, fldname, p.GetVal(slot, fldname))
		}
		p.Delete(slot)
		destslot++
	}
}

func (p *BTPage) fldpos(slot int, fldname string) int {
	offset := p.layout.Offset(fldname)
	return p.slotpos(slot) + offset
}

func (p *BTPage) slotpos(slot int) int {
	slotsize := p.layout.SlotSize()
	return intSize + intSize + (slot * slotsize)
}
