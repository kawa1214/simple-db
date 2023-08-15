package materialize

import (
	"github.com/kawa1214/simple-db/pkg/db/query"
	"github.com/kawa1214/simple-db/pkg/db/record"
)

// SortScan is the scan class for the sort operator.
type SortScan struct {
	s1, s2, currentScan query.UpdateScan
	comp                *RecordComparator
	hasMore1, hasMore2  bool
	savedPosition       []*record.RID
}

func NewSortScan(runs []*TempTable, comp *RecordComparator) *SortScan {
	ss := &SortScan{
		s1:       runs[0].Open(),
		hasMore1: runs[0].Open().Next(),
		comp:     comp,
	}
	if len(runs) > 1 {
		ss.s2 = runs[1].Open()
		ss.hasMore2 = runs[1].Open().Next()
	}
	return ss
}

func (ss *SortScan) BeforeFirst() {
	ss.currentScan = nil
	ss.s1.BeforeFirst()
	ss.hasMore1 = ss.s1.Next()
	if ss.s2 != nil {
		ss.s2.BeforeFirst()
		ss.hasMore2 = ss.s2.Next()
	}
}

func (ss *SortScan) Next() bool {
	if ss.currentScan != nil {
		if ss.currentScan == ss.s1 {
			ss.hasMore1 = ss.s1.Next()
		} else if ss.currentScan == ss.s2 {
			ss.hasMore2 = ss.s2.Next()
		}
	}

	if !ss.hasMore1 && !ss.hasMore2 {
		return false
	} else if ss.hasMore1 && ss.hasMore2 {
		if ss.comp.Compare(ss.s1, ss.s2) < 0 {
			ss.currentScan = ss.s1
		} else {
			ss.currentScan = ss.s2
		}
	} else if ss.hasMore1 {
		ss.currentScan = ss.s1
	} else if ss.hasMore2 {
		ss.currentScan = ss.s2
	}
	return true
}

func (ss *SortScan) Close() {
	ss.s1.Close()
	if ss.s2 != nil {
		ss.s2.Close()
	}
}

func (ss *SortScan) GetVal(fldName string) *record.Constant {
	return ss.currentScan.GetVal(fldName)
}

func (ss *SortScan) GetInt(fldName string) int {
	return ss.currentScan.GetInt(fldName)
}

func (ss *SortScan) GetString(fldName string) string {
	return ss.currentScan.GetString(fldName)
}

func (ss *SortScan) HasField(fldName string) bool {
	return ss.currentScan.HasField(fldName)
}

func (ss *SortScan) SavePosition() {
	rid1 := ss.s1.GetRid()
	var rid2 *record.RID
	if ss.s2 != nil {
		rid2 = ss.s2.GetRid()
	}
	ss.savedPosition = []*record.RID{rid1, rid2}
}

func (ss *SortScan) RestorePosition() {
	rid1 := ss.savedPosition[0]
	rid2 := ss.savedPosition[1]
	ss.s1.MoveToRid(rid1)
	if rid2 != nil {
		ss.s2.MoveToRid(rid2)
	}
}
