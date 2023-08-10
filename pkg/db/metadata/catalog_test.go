package metadata

import (
	"testing"

	"github.com/kawa1214/simple-db/pkg/db/buffer"
	"github.com/kawa1214/simple-db/pkg/db/file"
	"github.com/kawa1214/simple-db/pkg/db/log"
	"github.com/kawa1214/simple-db/pkg/db/record"
	"github.com/kawa1214/simple-db/pkg/db/tx"
	"github.com/kawa1214/simple-db/pkg/util"
)

func TestCatalog(t *testing.T) {
	rootDir := util.ProjectRootDir()
	dir := rootDir + "/.tmp"
	fm := file.NewFileMgr(dir, 400)
	lm := log.NewLogMgr(fm, "testlogfile")
	bm := buffer.NewBufferMgr(fm, lm, 8)
	tx := tx.NewTransaction(fm, lm, bm)
	tm := NewTableMgr(false, tx)

	tcatLayout := tm.GetLayout("tblcat", tx)

	t.Logf("Here are all the tables and their lengths.")
	ts := record.NewTableScan(tx, "tblcat", tcatLayout)
	for ts.Next() {
		tname := ts.GetString("tblname")
		slotsize := ts.GetInt("slotsize")
		t.Logf("%s %d\n", tname, slotsize)
	}
	ts.Close()

	t.Logf("Here are the fields for each table and their offsets")
	fcatLayout := tm.GetLayout("fldcat", tx)
	ts = record.NewTableScan(tx, "fldcat", fcatLayout)
	for ts.Next() {
		tname := ts.GetString("tblname")
		fname := ts.GetString("fldname")
		offset := ts.GetInt("offset")
		t.Logf("%s %s %d\n", tname, fname, offset)
	}
	ts.Close()

	t.Error()
}
