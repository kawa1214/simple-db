package buffer

import (
	"testing"

	"github.com/kawa1214/simple-db/pkg/db/file"
	"github.com/kawa1214/simple-db/pkg/db/log"
	"github.com/kawa1214/simple-db/pkg/util"
)

func TestBufferFile(t *testing.T) {
	rootDir := util.ProjectRootDir()
	dir := rootDir + "/.tmp"

	fm := file.NewFileMgr(dir, 400)
	lm := log.NewLogMgr(fm, "testlogfile")

	bm := NewBufferMgr(fm, lm, 8)
	blk := file.NewBlockId("testfile", 2)
	pos1 := 88

	b1, err := bm.Pin(blk)
	if err != nil {
		t.Error(err)
	}
	p1 := b1.Contents()
	p1.SetString(pos1, "abcdefghijklm")
	size := file.MaxLength(len("abcdefghijklm"))
	pos2 := pos1 + size
	p1.SetInt(pos2, 345)
	b1.SetModified(1, 0)
	bm.Unpin(b1)

	b2, err := bm.Pin(blk)
	if err != nil {
		t.Error(err)
	}
	p2 := b2.Contents()
	t.Logf("offset %v, contains %v", pos2, p2.GetInt(pos2))
	t.Logf("offset %v contains %v", pos1, p2.GetString(pos1))
	bm.Unpin(b2)

	t.Error()
}
