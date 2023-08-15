package index

import "github.com/kawa1214/simple-db/pkg/db/record"

// IndexSelectScan implements the Scan interface for the select relational algebra operator.
type IndexSelectScan struct {
	idx Index
	ts  *record.TableScan
	val *record.Constant
}

// NewIndexSelectScan creates a new IndexSelectScan instance.
func NewIndexSelectScan(ts *record.TableScan, idx Index, val *record.Constant) *IndexSelectScan {
	scan := &IndexSelectScan{ts: ts, idx: idx, val: val}
	scan.BeforeFirst()
	return scan
}

// BeforeFirst positions the scan before the first record.
func (s *IndexSelectScan) BeforeFirst() {
	s.idx.BeforeFirst(s.val)
}

// Next moves to the next record.
func (s *IndexSelectScan) Next() bool {
	ok := s.idx.Next()
	if ok {
		rid := s.idx.GetDataRid()
		s.ts.MoveToRid(rid)
	}
	return ok
}

// GetInt returns the integer value of the specified field.
func (s *IndexSelectScan) GetInt(fldname string) int {
	return s.ts.GetInt(fldname)
}

// GetString returns the string value of the specified field.
func (s *IndexSelectScan) GetString(fldname string) string {
	return s.ts.GetString(fldname)
}

// GetVal returns the value of the specified field.
func (s *IndexSelectScan) GetVal(fldname string) *record.Constant {
	return s.ts.GetVal(fldname)
}

// HasField checks if the data record has the specified field.
func (s *IndexSelectScan) HasField(fldname string) bool {
	return s.ts.HasField(fldname)
}

// Close closes the scan.
func (s *IndexSelectScan) Close() {
	s.idx.Close()
	s.ts.Close()
}
