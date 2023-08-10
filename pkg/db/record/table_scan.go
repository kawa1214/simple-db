package record

import (
	"github.com/kawa1214/simple-db/pkg/db/file"
	"github.com/kawa1214/simple-db/pkg/db/tx"
)

type TableScan struct {
	tx          *tx.Transaction
	layout      *Layout
	rp          *RecordPage
	filename    string
	currentslot int
}

func NewTableScan(tx *tx.Transaction, tblname string, layout *Layout) *TableScan {
	ts := &TableScan{
		tx:       tx,
		layout:   layout,
		filename: tblname + ".tbl",
	}

	if tx.Size(ts.filename) == 0 {
		ts.moveToNewBlock()
	} else {
		ts.moveToBlock(0)
	}
	return ts
}

func (ts *TableScan) BeforeFirst() {
	ts.moveToBlock(0)
}

func (ts *TableScan) Next() bool {
	ts.currentslot = ts.rp.NextAfter(ts.currentslot)
	for ts.currentslot < 0 {
		if ts.atLastBlock() {
			return false
		}
		ts.moveToBlock(ts.rp.Block().Number() + 1)
		ts.currentslot = ts.rp.NextAfter(ts.currentslot)
	}
	return true
}

func (ts *TableScan) GetInt(fldname string) int {
	return ts.rp.GetInt(ts.currentslot, fldname)
}

func (ts *TableScan) GetString(fldname string) string {
	return ts.rp.GetString(ts.currentslot, fldname)
}

func (ts *TableScan) GetVal(fldname string) *Constant {
	schemaType, err := ts.layout.Schema().Type(fldname)
	if err != nil {
		panic(err)
	}
	if schemaType == Integer {
		return NewConstant(ts.GetInt(fldname))
	}
	return NewConstant(ts.GetString(fldname))
}

func (ts *TableScan) HasField(fldname string) bool {
	return ts.layout.Schema().HasField(fldname)
}

func (ts *TableScan) Close() {
	if ts.rp != nil {
		ts.tx.Unpin(ts.rp.Block())
	}
}

func (ts *TableScan) SetInt(fldname string, val int) {
	ts.rp.SetInt(ts.currentslot, fldname, val)
}

func (ts *TableScan) SetString(fldname string, val string) {
	ts.rp.SetString(ts.currentslot, fldname, val)
}

func (ts *TableScan) SetVal(fldname string, val *Constant) {
	schemaType, err := ts.layout.Schema().Type(fldname)
	if err != nil {
		panic(err)
	}
	if schemaType == Integer {
		ts.SetInt(fldname, val.AsInt())
	} else {
		ts.SetString(fldname, val.AsString())
	}
}

func (ts *TableScan) Insert() {
	ts.currentslot = ts.rp.InsertAfter(ts.currentslot)
	for ts.currentslot < 0 {
		if ts.atLastBlock() {
			ts.moveToNewBlock()
		} else {
			ts.moveToBlock(ts.rp.Block().Number() + 1)
		}
		ts.currentslot = ts.rp.InsertAfter(ts.currentslot)
	}
}

func (ts *TableScan) Delete() {
	ts.rp.Delete(ts.currentslot)
}

func (ts *TableScan) MoveToRid(rid *RID) {
	ts.Close()
	blk := file.NewBlockId(ts.filename, rid.BlockNumber())
	ts.rp = NewRecordPage(ts.tx, blk, ts.layout)
	ts.currentslot = rid.Slot()
}

func (ts *TableScan) GetRid() *RID {
	return NewRID(ts.rp.Block().Number(), ts.currentslot)
}

func (ts *TableScan) moveToBlock(blknum int) {
	ts.Close()
	blk := file.NewBlockId(ts.filename, blknum)
	ts.rp = NewRecordPage(ts.tx, blk, ts.layout)
	ts.currentslot = -1
}

func (ts *TableScan) moveToNewBlock() {
	ts.Close()
	blk := ts.tx.Append(ts.filename)
	ts.rp = NewRecordPage(ts.tx, blk, ts.layout)
	ts.rp.Format()
	ts.currentslot = -1
}

func (ts *TableScan) atLastBlock() bool {
	return ts.rp.Block().Number() == ts.tx.Size(ts.filename)-1
}
