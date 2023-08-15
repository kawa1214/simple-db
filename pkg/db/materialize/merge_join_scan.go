package materialize

import (
	"github.com/kawa1214/simple-db/pkg/db/query"
	"github.com/kawa1214/simple-db/pkg/db/record"
)

// MergeJoinScan is the scan class for the mergejoin operator.
type MergeJoinScan struct {
	s1, s2   query.Scan
	fldname1 string
	fldname2 string
	joinval  *record.Constant
}

// NewMergeJoinScan creates a mergejoin scan for the two underlying sorted scans.
func NewMergeJoinScan(s1 query.Scan, s2 *SortScan, fldname1, fldname2 string) *MergeJoinScan {
	m := &MergeJoinScan{
		s1:       s1,
		s2:       s2,
		fldname1: fldname1,
		fldname2: fldname2,
	}
	m.BeforeFirst()
	return m
}

// Close closes the scan by closing the two underlying scans.
func (m *MergeJoinScan) Close() {
	m.s1.Close()
	m.s2.Close()
}

// BeforeFirst positions the scan before the first record.
func (m *MergeJoinScan) BeforeFirst() {
	m.s1.BeforeFirst()
	m.s2.BeforeFirst()
}

// Next moves to the next record.
func (m *MergeJoinScan) Next() bool {
	hasmore2 := m.s2.Next()
	if hasmore2 && m.s2.GetVal(m.fldname2).Equals(m.joinval) {
		return true
	}

	hasmore1 := m.s1.Next()
	if hasmore1 && m.s1.GetVal(m.fldname1).Equals(m.joinval) {
		m.s2.(*SortScan).RestorePosition() // Type assertion
		return true
	}

	for hasmore1 && hasmore2 {
		v1 := m.s1.GetVal(m.fldname1)
		v2 := m.s2.GetVal(m.fldname2)
		if v1.CompareTo(v2) < 0 {
			hasmore1 = m.s1.Next()
		} else if v1.CompareTo(v2) > 0 {
			hasmore2 = m.s2.Next()
		} else {
			m.s2.(*SortScan).SavePosition() // Type assertion
			m.joinval = m.s2.GetVal(m.fldname2)
			return true
		}
	}
	return false
}

// GetInt returns the integer value of the specified field.
func (m *MergeJoinScan) GetInt(fldname string) int {
	if m.s1.HasField(fldname) {
		return m.s1.GetInt(fldname)
	}
	return m.s2.GetInt(fldname)
}

// GetString returns the string value of the specified field.
func (m *MergeJoinScan) GetString(fldname string) string {
	if m.s1.HasField(fldname) {
		return m.s1.GetString(fldname)
	}
	return m.s2.GetString(fldname)
}

// GetVal returns the value of the specified field.
func (m *MergeJoinScan) GetVal(fldname string) *record.Constant {
	if m.s1.HasField(fldname) {
		return m.s1.GetVal(fldname)
	}
	return m.s2.GetVal(fldname)
}

// HasField returns true if the specified field is in either of the underlying scans.
func (m *MergeJoinScan) HasField(fldname string) bool {
	return m.s1.HasField(fldname) || m.s2.HasField(fldname)
}
