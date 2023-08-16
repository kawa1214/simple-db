package tx

import (
	"testing"

	"github.com/kawa1214/simple-db/pkg/db/file"
	"github.com/kawa1214/simple-db/pkg/db/log"
	"github.com/kawa1214/simple-db/pkg/util"
)

func TestPrintLogFile(t *testing.T) {
	rootDir := util.ProjectRootDir()
	dir := rootDir + "/.tmp"
	fm := file.NewFileMgr(dir, 400)
	filename := "testlogfile"
	lm := log.NewLogMgr(fm, filename)
	lastblock, _ := fm.Length(filename)
	lastblock--
	blk := file.NewBlockID(filename, lastblock)
	p := file.NewPage(fm.BlockSize())
	fm.Read(blk, p)

	iter := lm.Iterator()
	for iter.HasNext() {
		bytes := iter.Next()
		rec := CreateLogRecord(bytes)
		t.Log(rec)
	}

	t.Error()
}
