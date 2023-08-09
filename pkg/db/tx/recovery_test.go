package tx

// import (
// 	"fmt"
// 	"testing"

// 	"github.com/kawa1214/simple-db/pkg/db/buffer"
// 	"github.com/kawa1214/simple-db/pkg/db/file"
// )

// var (
// 	fm   *file.FileMgr
// 	bm   *buffer.BufferMgr
// 	blk0 *file.BlockId
// 	blk1 *file.BlockId
// )

// func TestRecovery(t *testing.T) {
// 	db = simpledb.New("recoverytest", 400, 8)
// 	fm = db.FileMgr()
// 	bm = db.BufferMgr()
// 	blk0 = simpledb.NewBlockId("testfile", 0)
// 	blk1 = simpledb.NewBlockId("testfile", 1)

// 	if fm.Length("testfile") == 0 {
// 		initialize()
// 		modify()
// 	} else {
// 		recover()
// 	}
// }

// func initialize() {
// 	tx1 := db.NewTx()
// 	tx2 := db.NewTx()
// 	tx1.Pin(blk0)
// 	tx2.Pin(blk1)
// 	pos := 0
// 	for i := 0; i < 6; i++ {
// 		tx1.SetInt(blk0, pos, pos, false)
// 		tx2.SetInt(blk1, pos, pos, false)
// 		pos += 4 // Go's int is 32-bits (4 bytes) by default
// 	}
// 	tx1.SetString(blk0, 30, "abc", false)
// 	tx2.SetString(blk1, 30, "def", false)
// 	tx1.Commit()
// 	tx2.Commit()
// 	printValues("After Initialization:")
// }

// func modify() {
// 	tx3 := db.NewTx()
// 	tx4 := db.NewTx()
// 	tx3.Pin(blk0)
// 	tx4.Pin(blk1)
// 	pos := 0
// 	for i := 0; i < 6; i++ {
// 		tx3.SetInt(blk0, pos, pos+100, true)
// 		tx4.SetInt(blk1, pos, pos+100, true)
// 		pos += 4 // Go's int is 32-bits (4 bytes) by default
// 	}
// 	tx3.SetString(blk0, 30, "uvw", true)
// 	tx4.SetString(blk1, 30, "xyz", true)
// 	bm.FlushAll(3)
// 	bm.FlushAll(4)
// 	printValues("After modification:")

// 	tx3.Rollback()
// 	printValues("After rollback:")
// }

// func recover() {
// 	tx := db.NewTx()
// 	tx.Recover()
// 	printValues("After recovery:")
// }

// func printValues(msg string) {
// 	fmt.Println(msg)
// 	p0 := simpledb.NewPage(fm.BlockSize())
// 	p1 := simpledb.NewPage(fm.BlockSize())
// 	fm.Read(blk0, p0)
// 	fm.Read(blk1, p1)
// 	pos := 0
// 	for i := 0; i < 6; i++ {
// 		fmt.Printf("%d ", p0.GetInt(pos))
// 		fmt.Printf("%d ", p1.GetInt(pos))
// 		pos += 4 // Go's int is 32-bits (4 bytes) by default
// 	}
// 	fmt.Printf("%s ", p0.GetString(30))
// 	fmt.Printf("%s ", p1.GetString(30))
// 	fmt.Println()
// }
