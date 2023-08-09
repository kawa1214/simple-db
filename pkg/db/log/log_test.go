package log

import (
	"fmt"
	"testing"

	"github.com/kawa1214/simple-db/pkg/db/file"
	"github.com/kawa1214/simple-db/pkg/util"
)

func TestLog(t *testing.T) {
	rootDir := util.ProjectRootDir()
	dir := rootDir + "/.tmp"
	fm := file.NewFileMgr(dir, 400)
	lm := NewLogMgr(fm, "testlogfile")

	printLogRecords(t, lm, "The initial empty log file:") // print an empty log file
	t.Logf("done")
	createRecords(t, lm, 1, 35)
	printLogRecords(t, lm, "The log file now has these records:")
	createRecords(t, lm, 36, 70)
	lm.Flush(65)
	printLogRecords(t, lm, "The log file now has these records:")

	t.Error()
}

func printLogRecords(t *testing.T, lm *LogMgr, msg string) {
	fmt.Println(msg)
	iter := lm.Iterator()
	for iter.HasNext() {
		rec := iter.Next()
		p := file.NewLogPage(rec)
		s := p.GetString(0)
		npos := file.MaxLength(len(s))
		val := p.GetInt(npos)
		t.Logf("[%s, %d]\n", s, val)
	}
	t.Logf("\n")
}

func createRecords(t *testing.T, lm *LogMgr, start int, end int) {
	t.Logf("Creating records: ")
	for i := start; i <= end; i++ {
		rec := createLogRecord(lm, fmt.Sprintf("record%d", i), i+100)
		lsn := lm.Append(rec)
		t.Logf("%d ", lsn)
	}
	fmt.Println()
}

func createLogRecord(lm *LogMgr, s string, n int) []byte {
	spos := 0
	npos := spos + file.MaxLength(len(s))
	p := file.NewPage(npos + 4) // assuming int is 4 bytes
	p.SetString(spos, s)
	p.SetInt(npos, n)
	return p.Contents().Bytes()
}
