package tx

import (
	// "fmt"
	// "fmt"
	"testing"
	"time"

	"github.com/kawa1214/simple-db/pkg/db/buffer"
	"github.com/kawa1214/simple-db/pkg/db/file"
	"github.com/kawa1214/simple-db/pkg/db/log"
	"github.com/kawa1214/simple-db/pkg/util"
)

func TestConcurrencyTest(t *testing.T) {
	rootDir := util.ProjectRootDir()
	dir := rootDir + "/.tmp"
	fm := file.NewFileMgr(dir, 400)
	lm := log.NewLogMgr(fm, "testlogfile")
	bm := buffer.NewBufferMgr(fm, lm, 8)
	go A(t, fm, lm, bm)
	go B(t, fm, lm, bm)
	go C(t, fm, lm, bm)
	time.Sleep(10 * time.Second) // Let the threads run for 10 seconds for this example

	// t.Error()
}

func A(t *testing.T, fm *file.FileMgr, lm *log.LogMgr, bm *buffer.BufferMgr) {
	blk1 := file.NewBlockID("testfile", 1)
	blk2 := file.NewBlockID("testfile", 2)
	txA := NewTransaction(fm, lm, bm)
	txA.Pin(blk1)
	txA.Pin(blk2)
	t.Log("Tx A: request slock 1")
	txA.GetInt(blk1, 0)
	t.Log("Tx A: receive slock 1")
	time.Sleep(1 * time.Second)
	t.Log("Tx A: request slock 2")
	txA.GetInt(blk2, 0)
	t.Log("Tx A: receive slock 2")
	txA.Commit()
	t.Log("Tx A: commit")
}

func B(t *testing.T, fm *file.FileMgr, lm *log.LogMgr, bm *buffer.BufferMgr) {
	blk1 := file.NewBlockID("testfile", 1)
	blk2 := file.NewBlockID("testfile", 2)
	txB := NewTransaction(fm, lm, bm)
	txB.Pin(blk1)
	txB.Pin(blk2)
	t.Log("Tx B: request xlock 2")
	txB.SetInt(blk2, 0, 0, false)
	t.Log("Tx B: receive xlock 2")
	time.Sleep(1 * time.Second)
	t.Log("Tx B: request slock 1")
	txB.GetInt(blk1, 0)
	t.Log("Tx B: receive slock 1")
	txB.Commit()
	t.Log("Tx B: commit")
}

func C(t *testing.T, fm *file.FileMgr, lm *log.LogMgr, bm *buffer.BufferMgr) {
	blk1 := file.NewBlockID("testfile", 1)
	blk2 := file.NewBlockID("testfile", 2)
	txC := NewTransaction(fm, lm, bm)
	txC.Pin(blk1)
	txC.Pin(blk2)
	time.Sleep(500 * time.Millisecond)
	t.Log("Tx C: request xlock 1")
	txC.SetInt(blk1, 0, 0, false)
	t.Log("Tx C: receive xlock 1")
	time.Sleep(1 * time.Second)
	t.Log("Tx C: request slock 2")
	txC.GetInt(blk2, 0)
	t.Log("Tx C: receive slock 2")
	txC.Commit()
	t.Log("Tx C: commit")
}
