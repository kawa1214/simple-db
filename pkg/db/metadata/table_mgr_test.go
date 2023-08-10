package metadata

import (
	"fmt"
	"testing"

	"github.com/kawa1214/simple-db/pkg/db/buffer"
	"github.com/kawa1214/simple-db/pkg/db/file"
	"github.com/kawa1214/simple-db/pkg/db/log"
	"github.com/kawa1214/simple-db/pkg/db/record"
	"github.com/kawa1214/simple-db/pkg/db/tx"
	"github.com/kawa1214/simple-db/pkg/util"
)

func TestTableMgr(t *testing.T) {
	rootDir := util.ProjectRootDir()
	dir := rootDir + "/.tmp"
	fm := file.NewFileMgr(dir, 400)
	lm := log.NewLogMgr(fm, "testlogfile")
	bm := buffer.NewBufferMgr(fm, lm, 8)
	tx := tx.NewTransaction(fm, lm, bm)
	tm := NewTableMgr(true, tx)

	sch := record.NewSchema()
	sch.AddIntField("A")
	sch.AddStringField("B", 9)
	tm.CreateTable("MyTable", sch, tx)

	layout := tm.GetLayout("MyTable", tx)
	size := layout.SlotSize()
	sch2 := layout.Schema()

	t.Logf("MyTable has slot size %d\n", size)
	t.Logf("Its fields are:\n")

	for _, fldname := range sch2.Fields {
		var typeStr string
		sch2Type, err := sch2.Type(fldname)
		if err != nil {
			t.Error(err)
		}
		if sch2Type == record.Integer {
			typeStr = "int"
		} else {
			strlen, err := sch2.Length(fldname)
			if err != nil {
				t.Error(err)
			}
			typeStr = fmt.Sprintf("varchar(%d)", strlen)
		}
		t.Logf("%s: %s\n", fldname, typeStr)
	}
	tx.Commit()

	t.Error()
}
