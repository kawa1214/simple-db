package file

import (
	"testing"

	"github.com/kawa1214/simple-db/pkg/util"
)

func TestFileMgr(t *testing.T) {
	rootDir := util.ProjectRootDir()
	dir := rootDir + "/.tmp"
	fm := NewFileMgr(dir, 400)
	blk := NewBlockId("testfile", 2)
	pos1 := 88

	p1 := NewPage(fm.BlockSize())
	p1.SetString(pos1, "abcdefghijklm")
	size := MaxLength(len("abcdefghijklm"))
	pos2 := pos1 + size
	p1.SetInt(pos2, 345)
	if err := fm.Write(blk, p1); err != nil {
		t.Error(err)
	}

	p2 := NewPage(fm.BlockSize())
	if err := fm.Read(blk, p2); err != nil {
		t.Error(err)
	}

	t.Logf("offset %d contains %d\n", pos2, p2.GetInt(pos2))
	// offset 144 contains 345
	t.Logf("offset %d contains %s\n", pos1, p2.GetString(pos1))
	// offset 88 contains abcdefghijklm
}
