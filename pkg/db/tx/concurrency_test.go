package tx

// import (
// 	"fmt"
// 	"time"

// 	"github.com/kawa1214/simple-db/pkg/db/buffer"
// 	"github.com/kawa1214/simple-db/pkg/db/file"
// 	"github.com/kawa1214/simple-db/pkg/db/log"
// )

// var fm *file.FileMgr
// var lm *log.LogMgr
// var bm *buffer.BufferMgr

// func main() {
// 	// initialize the database system
// 	db := server.NewSimpleDB("concurrencytest", 400, 8)
// 	fm = db.FileMgr()
// 	lm = db.LogMgr()
// 	bm = db.BufferMgr()
// 	go A()
// 	go B()
// 	go C()
// 	time.Sleep(10 * time.Second) // Let the threads run for 10 seconds for this example
// }

// func A() {
// 	defer func() {
// 		if r := recover(); r != nil {
// 			fmt.Println("Recovered from", r)
// 		}
// 	}()
// 	blk1 := file.NewBlockId("testfile", 1)
// 	blk2 := file.NewBlockId("testfile", 2)
// 	txA := NewTransaction(fm, lm, bm)
// 	txA.Pin(blk1)
// 	txA.Pin(blk2)
// 	fmt.Println("Tx A: request slock 1")
// 	txA.GetInt(blk1, 0)
// 	fmt.Println("Tx A: receive slock 1")
// 	time.Sleep(1 * time.Second)
// 	fmt.Println("Tx A: request slock 2")
// 	txA.GetInt(blk2, 0)
// 	fmt.Println("Tx A: receive slock 2")
// 	txA.Commit()
// 	fmt.Println("Tx A: commit")
// }

// func B() {
// 	defer func() {
// 		if r := recover(); r != nil {
// 			fmt.Println("Recovered from", r)
// 		}
// 	}()
// 	blk1 := file.NewBlockId("testfile", 1)
// 	blk2 := file.NewBlockId("testfile", 2)
// 	txB := NewTransaction(fm, lm, bm)
// 	txB.Pin(blk1)
// 	txB.Pin(blk2)
// 	fmt.Println("Tx B: request xlock 2")
// 	txB.SetInt(blk2, 0, 0, false)
// 	fmt.Println("Tx B: receive xlock 2")
// 	time.Sleep(1 * time.Second)
// 	fmt.Println("Tx B: request slock 1")
// 	txB.GetInt(blk1, 0)
// 	fmt.Println("Tx B: receive slock 1")
// 	txB.Commit()
// 	fmt.Println("Tx B: commit")
// }

// func C() {
// 	defer func() {
// 		if r := recover(); r != nil {
// 			fmt.Println("Recovered from", r)
// 		}
// 	}()
// 	blk1 := file.NewBlockId("testfile", 1)
// 	blk2 := file.NewBlockId("testfile", 2)
// 	txC := NewTransaction(fm, lm, bm)
// 	txC.Pin(blk1)
// 	txC.Pin(blk2)
// 	time.Sleep(500 * time.Millisecond)
// 	fmt.Println("Tx C: request xlock 1")
// 	txC.SetInt(blk1, 0, 0, false)
// 	fmt.Println("Tx C: receive xlock 1")
// 	time.Sleep(1 * time.Second)
// 	fmt.Println("Tx C: request slock 2")
// 	txC.GetInt(blk2, 0)
// 	fmt.Println("Tx C: receive slock 2")
// 	txC.Commit()
// 	fmt.Println("Tx C: commit")
// }
