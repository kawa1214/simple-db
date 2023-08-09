package tx

// import (
// 	"fmt"
// 	"testing"
// )

// func TestTx(t *testing.T) {
// 	db := simpledb.New("txtest", 400, 8)
// 	fm := db.FileMgr()
// 	lm := db.LogMgr()
// 	bm := db.BufferMgr()

// 	tx1 := simpledb.NewTransaction(fm, lm, bm)
// 	blk := simpledb.NewBlockId("testfile", 1)
// 	tx1.Pin(blk)
// 	tx1.SetInt(blk, 80, 1, false)
// 	tx1.SetString(blk, 40, "one", false)
// 	tx1.Commit()

// 	tx2 := simpledb.NewTransaction(fm, lm, bm)
// 	tx2.Pin(blk)
// 	ival := tx2.GetInt(blk, 80)
// 	sval := tx2.GetString(blk, 40)
// 	fmt.Printf("initial value at location 80 = %d\n", ival)
// 	fmt.Printf("initial value at location 40 = %s\n", sval)
// 	newival := ival + 1
// 	newsval := sval + "!"
// 	tx2.SetInt(blk, 80, newival, true)
// 	tx2.SetString(blk, 40, newsval, true)
// 	tx2.Commit()

// 	tx3 := simpledb.NewTransaction(fm, lm, bm)
// 	tx3.Pin(blk)
// 	fmt.Printf("new value at location 80 = %d\n", tx3.GetInt(blk, 80))
// 	fmt.Printf("new value at location 40 = %s\n", tx3.GetString(blk, 40))
// 	tx3.SetInt(blk, 80, 9999, true)
// 	fmt.Printf("pre-rollback value at location 80 = %d\n", tx3.GetInt(blk, 80))
// 	tx3.Rollback()

// 	tx4 := simpledb.NewTransaction(fm, lm, bm)
// 	tx4.Pin(blk)
// 	fmt.Printf("post-rollback at location 80 = %d\n", tx4.GetInt(blk, 80))
// 	tx4.Commit()

// }
