package tx

import (
	"testing"

	"github.com/kawa1214/simple-db/pkg/db/buffer"
	"github.com/kawa1214/simple-db/pkg/db/file"
	"github.com/kawa1214/simple-db/pkg/db/log"
	"github.com/kawa1214/simple-db/pkg/util"
)

func TestTx(t *testing.T) {
	rootDir := util.ProjectRootDir()
	dir := rootDir + "/.tmp"
	fm := file.NewFileMgr(dir, 400)
	lm := log.NewLogMgr(fm, "testlogfile")
	bm := buffer.NewBufferMgr(fm, lm, 8)

	tx1 := NewTransaction(fm, lm, bm)
	blk := file.NewBlockId("testfile", 1)
	tx1.Pin(blk)
	tx1.SetInt(blk, 80, 1, false)
	tx1.SetString(blk, 40, "one", false)
	tx1.Commit()

	tx2 := NewTransaction(fm, lm, bm)
	tx2.Pin(blk)
	ival := tx2.GetInt(blk, 80)
	sval := tx2.GetString(blk, 40)
	t.Logf("initial value at location 80 = %d\n", ival)
	t.Logf("initial value at location 40 = %s\n", sval)
	newival := ival + 1
	newsval := sval + "!"
	tx2.SetInt(blk, 80, newival, true)
	tx2.SetString(blk, 40, newsval, true)
	tx2.Commit()

	tx3 := NewTransaction(fm, lm, bm)
	tx3.Pin(blk)
	t.Logf("new value at location 80 = %d\n", tx3.GetInt(blk, 80))
	t.Logf("new value at location 40 = %s\n", tx3.GetString(blk, 40))
	tx3.SetInt(blk, 80, 9999, true)
	t.Logf("pre-rollback value at location 80 = %d\n", tx3.GetInt(blk, 80))
	tx3.Rollback()

	tx4 := NewTransaction(fm, lm, bm)
	tx4.Pin(blk)
	t.Logf("post-rollback at location 80 = %d\n", tx4.GetInt(blk, 80))
	tx4.Commit()

	t.Error()
}

func TestMoreTx(t *testing.T) {
	rootDir := util.ProjectRootDir()
	dir := rootDir + "/.tmp"
	fm := file.NewFileMgr(dir, 400)
	lm := log.NewLogMgr(fm, "testlogfile")
	bm := buffer.NewBufferMgr(fm, lm, 8)

	tx := NewTransaction(fm, lm, bm)
	blk := file.NewBlockId("testfile", 1)
	tx.Pin(blk)

	for i := 0; i < 50; i++ {
		tx.SetInt(blk, 80, 1, false)
	}
	tx.Commit()
}
