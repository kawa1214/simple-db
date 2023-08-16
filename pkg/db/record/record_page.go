package record

import (
	"github.com/kawa1214/simple-db/pkg/db/file"
	"github.com/kawa1214/simple-db/pkg/db/tx"
)

const (
	EMPTY = 0
	USED  = 1
)

type RecordPage struct {
	tx     *tx.Transaction
	blk    *file.BlockID
	layout *Layout
}

func NewRecordPage(t *tx.Transaction, b *file.BlockID, l *Layout) *RecordPage {
	t.Pin(b)
	return &RecordPage{tx: t, blk: b, layout: l}
}

func (rp *RecordPage) GetInt(slot int, fldname string) int {
	fldpos := rp.offset(slot) + rp.layout.Offset(fldname)
	return rp.tx.GetInt(rp.blk, fldpos)
}

func (rp *RecordPage) GetString(slot int, fldname string) string {
	fldpos := rp.offset(slot) + rp.layout.Offset(fldname)
	return rp.tx.GetString(rp.blk, fldpos)
}

func (rp *RecordPage) SetInt(slot int, fldname string, val int) {
	fldpos := rp.offset(slot) + rp.layout.Offset(fldname)
	rp.tx.SetInt(rp.blk, fldpos, val, true)
}

func (rp *RecordPage) SetString(slot int, fldname string, val string) {
	fldpos := rp.offset(slot) + rp.layout.Offset(fldname)
	rp.tx.SetString(rp.blk, fldpos, val, true)
}

func (rp *RecordPage) Delete(slot int) {
	rp.setFlag(slot, EMPTY)
}

func (rp *RecordPage) Format() {
	slot := 0
	for rp.isValidSlot(slot) {
		rp.tx.SetInt(rp.blk, rp.offset(slot), EMPTY, false)
		sch := rp.layout.Schema()
		for _, fldname := range sch.Fields {
			fldpos := rp.offset(slot) + rp.layout.Offset(fldname)
			schemaType, err := sch.Type(fldname)
			if err != nil {
				panic(err)
			}
			if schemaType == Integer {
				rp.tx.SetInt(rp.blk, fldpos, 0, false)
			} else {
				rp.tx.SetString(rp.blk, fldpos, "", false)
			}
		}
		slot++
	}
}

func (rp *RecordPage) NextAfter(slot int) int {
	return rp.searchAfter(slot, USED)
}

func (rp *RecordPage) InsertAfter(slot int) int {
	newslot := rp.searchAfter(slot, EMPTY)
	if newslot >= 0 {
		rp.setFlag(newslot, USED)
	}
	return newslot
}

func (rp *RecordPage) Block() *file.BlockID {
	return rp.blk
}

func (rp *RecordPage) setFlag(slot int, flag int) {
	rp.tx.SetInt(rp.blk, rp.offset(slot), flag, true)
}

func (rp *RecordPage) searchAfter(slot int, flag int) int {
	slot++
	for rp.isValidSlot(slot) {
		if rp.tx.GetInt(rp.blk, rp.offset(slot)) == flag {
			return slot
		}
		slot++
	}
	return -1
}

func (rp *RecordPage) isValidSlot(slot int) bool {
	return rp.offset(slot+1) <= rp.tx.BlockSize()
}

func (rp *RecordPage) offset(slot int) int {
	return slot * rp.layout.SlotSize()
}
