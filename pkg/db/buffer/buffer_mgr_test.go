package buffer

import (
	"testing"

	"github.com/kawa1214/simple-db/pkg/db/file"
	"github.com/kawa1214/simple-db/pkg/db/log"
	"github.com/kawa1214/simple-db/pkg/util"
)

func TestBufferMgr(t *testing.T) {
	// 最大8つのバッファを持つバッファマネージャを作成する
	// 1. バッファ1,2,3をピン留めする → 利用可能なバッファが0つなる
	// 2. バッファ2をピン留めを解除する → 利用可能なバッファが1つになる
	// 3. バッファ0を再度ピン留めする → 利用可能なバッファが0つになる
	// 4. バッファ1を再度ピン留めする → 利用可能なバッファが０のため10病後にタイムアウトする
	rootDir := util.ProjectRootDir()
	dir := rootDir + "/.tmp"

	fm := file.NewFileMgr(dir, 400)
	lm := log.NewLogMgr(fm, "testlogfile")

	bm := NewBufferMgr(fm, lm, 3)

	buff := make([]*Buffer, 6)
	block, err := bm.Pin(file.NewBlockID("testfile", 0))
	if err != nil {
		t.Error(err)
	}
	buff[0] = block
	block, err = bm.Pin(file.NewBlockID("testfile", 1))
	if err != nil {
		t.Error(err)
	}
	buff[1] = block
	block, err = bm.Pin(file.NewBlockID("testfile", 2))
	if err != nil {
		t.Error(err)
	}
	buff[2] = block

	bm.Unpin(buff[1])
	buff[1] = nil

	block, err = bm.Pin(file.NewBlockID("testfile", 0)) // block 0 pinned twice
	if err != nil {
		t.Error(err)
	}
	buff[3] = block
	block, err = bm.Pin(file.NewBlockID("testfile", 1)) // block 1 repinned
	if err != nil {
		t.Error(err)
	}
	buff[4] = block
	t.Logf("Available buffers: %v", bm.Available())
	// Available buffers: 0
	t.Logf("Attempting to pin block 3...")
	_, err = bm.Pin(file.NewBlockID("testfile", 3))
	if err != nil {
		t.Logf("Exception: No available buffers")
		// ここでエラーが発生する
		// すでに利用可能なバッファが0になっているため、10病後に強制的にタイムアウトする
		// Exception: No available buffers
	}
	bm.Unpin(buff[2])
	buff[2] = nil
	block, err = bm.Pin(file.NewBlockID("testfile", 3))
	if err != nil {
		t.Error(err)
	}
	buff[5] = block

	t.Logf("Final Buffer Allocation:")
	for i, b := range buff {
		if b != nil {
			t.Logf("buff[%v] pinned to block, %v", i, b.Block())
			// buff[0] pinned to block, [file testfile, block 0]
			// buff[3] pinned to block, [file testfile, block 0]
			// buff[4] pinned to block, [file testfile, block 1]
			// buff[5] pinned to block, [file testfile, block 3]
		}
	}

	// t.Error()
}
