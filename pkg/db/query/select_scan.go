package query

import "github.com/kawa1214/simple-db/pkg/db/record"

type SelectScan struct {
	s    Scan
	pred Predicate
}

// NewSelectScan creates a new SelectScan instance
func NewSelectScan(s Scan, pred Predicate) *SelectScan {
	return &SelectScan{
		s:    s,
		pred: pred,
	}
}

// BeforeFirst positions the scan before its first record
func (ss *SelectScan) BeforeFirst() {
	ss.s.BeforeFirst()
}

// Next moves the scan to the next record and returns true if there is such a record
func (ss *SelectScan) Next() bool {
	for ss.s.Next() {
		if ss.pred.IsSatisfied(ss.s) {
			return true
		}
	}
	return false
}

func (ss *SelectScan) GetInt(fldname string) int {
	return ss.s.GetInt(fldname)
}

func (ss *SelectScan) GetString(fldname string) string {
	return ss.s.GetString(fldname)
}

func (ss *SelectScan) GetVal(fldname string) *record.Constant {
	return ss.s.GetVal(fldname)
}

func (ss *SelectScan) HasField(fldname string) bool {
	return ss.s.HasField(fldname)
}

func (ss *SelectScan) Close() {
	ss.s.Close()
}

func (ss *SelectScan) SetInt(fldname string, val int) {
	us := ss.s.(UpdateScan)
	us.SetInt(fldname, val)
}

func (ss *SelectScan) SetString(fldname string, val string) {
	us := ss.s.(UpdateScan)
	us.SetString(fldname, val)
}

func (ss *SelectScan) SetVal(fldname string, val *record.Constant) {
	us := ss.s.(UpdateScan)
	us.SetVal(fldname, val)
}

func (ss *SelectScan) Delete() {
	us := ss.s.(UpdateScan)
	us.Delete()
}

func (ss *SelectScan) Insert() {
	us := ss.s.(UpdateScan)
	us.Insert()
}

func (ss *SelectScan) GetRid() *record.RID {
	us := ss.s.(UpdateScan)
	return us.GetRid()
}

func (ss *SelectScan) MoveToRid(rid *record.RID) {
	us := ss.s.(UpdateScan)
	us.MoveToRid(rid)
}
