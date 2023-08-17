package record

import (
	"testing"
)

func TestRecord(t *testing.T) {
	// rootDir := util.ProjectRootDir()
	// dir := rootDir + "/.tmp"
	// fm := file.NewFileMgr(dir, 400)
	// lm := log.NewLogMgr(fm, "testlogfile")
	// bm := buffer.NewBufferMgr(fm, lm, 8)
	// tx := tx.NewTransaction(fm, lm, bm)

	// sch := NewSchema()
	// sch.AddIntField("A")
	// sch.AddStringField("B", 9)
	// layout := NewLayoutFromSchema(sch)
	// for _, fldname := range layout.Schema().Fields {
	// 	offset := layout.Offset(fldname)
	// 	t.Logf("%s has offset %d\n", fldname, offset)
	// }

	// blk := tx.Append("testfile")
	// tx.Pin(blk)
	// rp := NewRecordPage(tx, blk, layout)
	// rp.Format()

	// t.Logf("Filling the page with random records.")
	// slot := rp.InsertAfter(-1)
	// for slot >= 0 {
	// 	n := int(math.Round(rand.Float64() * 50))
	// 	rp.SetInt(slot, "A", n)
	// 	rp.SetString(slot, "B", "rec"+fmt.Sprintf("%d", n))
	// 	t.Logf("inserting into slot %d: {%d, rec%d}\n", slot, n, n)
	// 	slot = rp.InsertAfter(slot)
	// }

	// // fmt.Println("Deleting these records, whose A-values are less than 25.")
	// t.Logf("page has %d slots\n", layout.SlotSize())
	// count := 0
	// slot = rp.NextAfter(-1)
	// for slot >= 0 {
	// 	a := rp.GetInt(slot, "A")
	// 	b := rp.GetString(slot, "B")
	// 	if a < 25 {
	// 		count++
	// 		t.Logf("slot %d: {%d, %s}\n", slot, a, b)
	// 		rp.Delete(slot)
	// 	}
	// 	slot = rp.NextAfter(slot)
	// }
	// t.Logf("page has %d slots\n", layout.SlotSize())

	// t.Logf("Here are the remaining records.")
	// slot = rp.NextAfter(-1)
	// for slot >= 0 {
	// 	a := rp.GetInt(slot, "A")
	// 	b := rp.GetString(slot, "B")
	// 	t.Logf("slot %d: {%d, %s}\n", slot, a, b)
	// 	slot = rp.NextAfter(slot)
	// }
	// tx.Unpin(blk)
	// tx.Commit()

	// t.Error()
}
