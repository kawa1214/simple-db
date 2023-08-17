package buffer

import (
	"testing"

	"github.com/kawa1214/simple-db/pkg/db/file"
	"github.com/kawa1214/simple-db/pkg/db/log"
	"github.com/kawa1214/simple-db/pkg/util"
)

func TestBuffer(t *testing.T) {
	// 最初に実行したときは1となり、それ以降の実行で1ずつインクリメントされる
	rootDir := util.ProjectRootDir()
	dir := rootDir + "/.tmp"

	fm := file.NewFileMgr(dir, 400)
	lm := log.NewLogMgr(fm, "testlogfile")

	bm := NewBufferMgr(fm, lm, 3)

	buff1, err := bm.Pin(file.NewBlockID("testfile", 1))
	if err != nil {
		t.Error(err)
	}
	p := buff1.Contents()
	n := p.GetInt(80)
	p.SetInt(80, n+1)
	buff1.SetModified(1, 0)
	t.Logf("The new value is %v", n+1)
	bm.Unpin(buff1)

	// これらのピンのいずれかがbuff1をディスクにフラッシュする：
	// 新しいブロックをピン留めする→バッファがいっぱいになる→buff1がディスクにフラッシュされる
	// もしくは、bm.FlushAll(1)でもbuff1がディスクにフラッシュされる
	buff2, err := bm.Pin(file.NewBlockID("testfile", 2))
	if err != nil {
		t.Error(err)
	}
	_, err = bm.Pin(file.NewBlockID("testfile", 3))
	if err != nil {
		t.Error(err)
	}
	_, err = bm.Pin(file.NewBlockID("testfile", 4))
	if err != nil {
		t.Error(err)
	}

	bm.Unpin(buff2)
	buff2, err = bm.Pin(file.NewBlockID("testfile", 1))
	if err != nil {
		t.Error(err)
	}
	p2 := buff2.Contents()
	// この変更はディスクに書き込まれない。
	p2.SetInt(80, 9999)
	buff2.SetModified(1, 0)

	// t.Error()
}
