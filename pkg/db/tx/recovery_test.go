package tx

import (
	"testing"

	"github.com/kawa1214/simple-db/pkg/db/buffer"
	"github.com/kawa1214/simple-db/pkg/db/file"
	"github.com/kawa1214/simple-db/pkg/db/log"
	"github.com/kawa1214/simple-db/pkg/util"
)

func TestRecovery(t *testing.T) {
	rootDir := util.ProjectRootDir()
	dir := rootDir + "/.tmp"
	fm := file.NewFileMgr(dir, 400)
	lm := log.NewLogMgr(fm, "testlogfile")
	bm := buffer.NewBufferMgr(fm, lm, 8)
	blk0 := file.NewBlockID("testfile", 0)
	blk1 := file.NewBlockID("testfile", 1)

	len, err := fm.Length("testfile")
	if err != nil {
		t.Error(err)
	}

	if len == 0 {
		initialize(t, fm, lm, bm, blk0, blk1)
		modify(t, fm, lm, bm, blk0, blk1)
	} else {
		recover(t, fm, lm, bm, blk0, blk1)
	}
	t.Error()
}

func initialize(t *testing.T, fm *file.FileMgr, lm *log.LogMgr, bm *buffer.BufferMgr, blk0 *file.BlockID, blk1 *file.BlockID) {
	tx1 := NewTransaction(fm, lm, bm)
	tx2 := NewTransaction(fm, lm, bm)
	tx1.Pin(blk0)
	tx2.Pin(blk1)
	pos := 0
	for i := 0; i < 6; i++ {
		tx1.SetInt(blk0, pos, pos, false)
		tx2.SetInt(blk1, pos, pos, false)
		pos += 4 // Go's int is 32-bits (4 bytes) by default
	}
	tx1.SetString(blk0, 30, "abc", false)
	tx2.SetString(blk1, 30, "def", false)
	tx1.Commit()
	tx2.Commit()
	printValues(t, fm, lm, bm, blk0, blk1, "After Initialization:")
}

func modify(t *testing.T, fm *file.FileMgr, lm *log.LogMgr, bm *buffer.BufferMgr, blk0 *file.BlockID, blk1 *file.BlockID) {
	tx3 := NewTransaction(fm, lm, bm)
	tx4 := NewTransaction(fm, lm, bm)
	tx3.Pin(blk0)
	tx4.Pin(blk1)
	pos := 0
	for i := 0; i < 6; i++ {
		tx3.SetInt(blk0, pos, pos+100, true)
		tx4.SetInt(blk1, pos, pos+100, true)
		pos += 4 // Go's int is 32-bits (4 bytes) by default
	}
	tx3.SetString(blk0, 30, "uvw", true)
	tx4.SetString(blk1, 30, "xyz", true)
	bm.FlushAll(3)
	bm.FlushAll(4)
	printValues(t, fm, lm, bm, blk0, blk1, "After modification:")

	tx3.Rollback()
	printValues(t, fm, lm, bm, blk0, blk1, "After rollback:")
}

func recover(t *testing.T, fm *file.FileMgr, lm *log.LogMgr, bm *buffer.BufferMgr, blk0 *file.BlockID, blk1 *file.BlockID) {
	tx := NewTransaction(fm, lm, bm)
	tx.Recover()
	printValues(t, fm, lm, bm, blk0, blk1, "After recovery:")
}

func printValues(t *testing.T, fm *file.FileMgr, lm *log.LogMgr, bm *buffer.BufferMgr, blk0 *file.BlockID, blk1 *file.BlockID, msg string) {
	t.Logf(msg)
	p0 := file.NewPage(fm.BlockSize())
	p1 := file.NewPage(fm.BlockSize())
	fm.Read(blk0, p0)
	fm.Read(blk1, p1)
	pos := 0
	for i := 0; i < 6; i++ {
		t.Logf("%d ", p0.GetInt(pos))
		t.Logf("%d ", p1.GetInt(pos))
		pos += 4 // Go's int is 32-bits (4 bytes) by default
	}
	t.Logf("%s ", p0.GetString(30))
	t.Logf("%s ", p1.GetString(30))
	t.Logf("")
}
