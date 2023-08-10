package metadata

import (
	"sync"

	"github.com/kawa1214/simple-db/pkg/db/record"
	"github.com/kawa1214/simple-db/pkg/db/tx"
)

// StatMgr is responsible for keeping statistical information about each table.
type StatMgr struct {
	tblMgr     *TableMgr
	tablestats map[string]*StatInfo
	numcalls   int
	mu         sync.Mutex
}

// NewStatMgr creates the statistics manager.
func NewStatMgr(tblMgr *TableMgr, tx *tx.Transaction) *StatMgr {
	sm := &StatMgr{
		tblMgr:     tblMgr,
		tablestats: make(map[string]*StatInfo),
	}
	sm.refreshStatistics(tx)
	return sm
}

// GetStatInfo returns the statistical information about the specified table.
func (sm *StatMgr) GetStatInfo(tblname string, layout *record.Layout, tx *tx.Transaction) *StatInfo {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	sm.numcalls++
	if sm.numcalls > 100 {
		sm.refreshStatistics(tx)
	}
	si, ok := sm.tablestats[tblname]
	if !ok {
		si = sm.calcTableStats(tblname, layout, tx)
		sm.tablestats[tblname] = si
	}
	return si
}

func (sm *StatMgr) refreshStatistics(tx *tx.Transaction) {
	sm.tablestats = make(map[string]*StatInfo)
	sm.numcalls = 0
	tcatlayout := sm.tblMgr.GetLayout("tblcat", tx)
	tcat := record.NewTableScan(tx, "tblcat", tcatlayout)
	for tcat.Next() {
		tblname := tcat.GetString("tblname")
		layout := sm.tblMgr.GetLayout(tblname, tx)
		si := sm.calcTableStats(tblname, layout, tx)
		sm.tablestats[tblname] = si
	}
	tcat.Close()
}

func (sm *StatMgr) calcTableStats(tblname string, layout *record.Layout, tx *tx.Transaction) *StatInfo {
	var numRecs, numblocks int
	ts := record.NewTableScan(tx, tblname, layout)
	for ts.Next() {
		numRecs++
		numblocks = ts.GetRid().BlockNumber() + 1
	}
	ts.Close()
	return NewStatInfo(numblocks, numRecs)
}
