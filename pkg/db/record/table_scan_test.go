package record

import (
	"testing"

	"github.com/kawa1214/simple-db/pkg/db/buffer"
	"github.com/kawa1214/simple-db/pkg/db/file"
	"github.com/kawa1214/simple-db/pkg/db/log"
	"github.com/kawa1214/simple-db/pkg/db/tx"
	"github.com/kawa1214/simple-db/pkg/util"
)

func TestTableScan(t *testing.T) {
	rootDir := util.ProjectRootDir()
	dir := rootDir + "/.tmp"
	fm := file.NewFileMgr(dir, 400)
	lm := log.NewLogMgr(fm, "testlogfile")
	bm := buffer.NewBufferMgr(fm, lm, 8)
	tx := tx.NewTransaction(fm, lm, bm)

	sch := NewSchema()
	sch.AddIntField("A")
	sch.AddStringField("B", 9)
	layout := NewLayoutFromSchema(sch)

	for _, fldname := range layout.Schema().Fields {
		offset := layout.Offset(fldname)
		t.Logf("%s has offset %d\n", fldname, offset)
	}

	t.Logf("table has %d slots\n", layout.SlotSize())
	ts := NewTableScan(tx, "T", layout)

	t.Logf("table has %d slots\n", layout.SlotSize())
	count := 0
	ts.BeforeFirst()
	for ts.Next() {
		a := ts.GetInt("A")
		b := ts.GetString("B")
		if a < 25 {
			count++
			t.Logf("slot %s: {%d, %s}\n", ts.GetRid(), a, b)
			ts.Delete()
		}
	}
	t.Logf("table has %d slots\n", layout.SlotSize())

	t.Logf("Here are the remaining records.")
	ts.BeforeFirst()
	for ts.Next() {
		a := ts.GetInt("A")
		b := ts.GetString("B")
		t.Logf("slot %s: {%d, %s}\n", ts.GetRid(), a, b)
	}
	ts.Close()
	tx.Commit()

	t.Error()
}
