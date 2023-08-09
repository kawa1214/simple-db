package log

import (
	"sync"

	"github.com/kawa1214/simple-db/pkg/db/file"
)

type LogMgr struct {
	fm           *file.FileMgr
	logfile      string
	logpage      *file.Page
	currentblk   *file.BlockId
	latestLSN    int
	lastSavedLSN int
	mutex        sync.Mutex
}

func NewLogMgr(fm *file.FileMgr, logfile string) *LogMgr {
	logPage := file.NewPage(fm.BlockSize())
	lm := &LogMgr{
		fm:      fm,
		logfile: logfile,
		logpage: logPage,
	}
	logsize, err := fm.Length(logfile)
	if err != nil {
		panic(err)
	}
	if logsize == 0 {
		lm.currentblk = lm.appendNewBlock()
	} else {
		lm.currentblk = file.NewBlockId(logfile, logsize-1)
		fm.Read(lm.currentblk, lm.logpage)
	}
	return lm
}

func (lm *LogMgr) Flush(lsn int) {
	if lsn >= lm.lastSavedLSN {
		lm.flushInternal()
	}
}

func (lm *LogMgr) flushInternal() {
	lm.fm.Write(lm.currentblk, lm.logpage)
	lm.lastSavedLSN = lm.latestLSN
}

func (lm *LogMgr) Append(logrec []byte) int {
	lm.mutex.Lock()
	defer lm.mutex.Unlock()

	boundary := lm.logpage.GetInt(0)
	recsize := len(logrec)
	bytesneeded := recsize + 4 // assuming int is 4 bytes
	if boundary-bytesneeded < 4 {
		lm.flushInternal()
		lm.currentblk = lm.appendNewBlock()
		boundary = lm.logpage.GetInt(0)
	}
	recpos := boundary - bytesneeded

	lm.logpage.SetBytes(recpos, logrec)
	lm.logpage.SetInt(0, recpos)
	lm.latestLSN++
	return lm.latestLSN
}

func (lm *LogMgr) appendNewBlock() *file.BlockId {
	blk, err := lm.fm.Append(lm.logfile)
	if err != nil {
		panic(err)
	}
	lm.logpage.SetInt(0, lm.fm.BlockSize())
	lm.fm.Write(blk, lm.logpage)
	return blk
}

func (lm *LogMgr) Iterator() *LogIterator {
	lm.flushInternal()
	return NewLogIterator(lm.fm, lm.currentblk)
}
