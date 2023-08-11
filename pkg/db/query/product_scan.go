package query

import "github.com/kawa1214/simple-db/pkg/db/record"

// ProductScan corresponds to the product relational algebra operator.
type ProductScan struct {
	s1, s2 Scan
}

// NewProductScan creates a product scan having the two underlying scans.
func NewProductScan(s1, s2 Scan) *ProductScan {
	ps := &ProductScan{s1: s1, s2: s2}
	ps.BeforeFirst()
	return ps
}

// BeforeFirst positions the scan before its first record. In particular, the LHS scan is positioned at its first record, and the RHS scan is positioned before its first record.
func (ps *ProductScan) BeforeFirst() {
	ps.s1.BeforeFirst()
	ps.s1.Next()
	ps.s2.BeforeFirst()
}

// Next moves the scan to the next record. The method moves to the next RHS record, if possible. Otherwise, it moves to the next LHS record and the first RHS record. If there are no more LHS records, the method returns false.
func (ps *ProductScan) Next() bool {
	if ps.s2.Next() {
		return true
	} else {
		ps.s2.BeforeFirst()
		return ps.s2.Next() && ps.s1.Next()
	}
}

// GetInt returns the integer value of the specified field. The value is obtained from whichever scan contains the field.
func (ps *ProductScan) GetInt(fldname string) int {
	if ps.s1.HasField(fldname) {
		return ps.s1.GetInt(fldname)
	}
	return ps.s2.GetInt(fldname)
}

// GetString returns the string value of the specified field. The value is obtained from whichever scan contains the field.
func (ps *ProductScan) GetString(fldname string) string {
	if ps.s1.HasField(fldname) {
		return ps.s1.GetString(fldname)
	}
	return ps.s2.GetString(fldname)
}

// GetVal returns the value of the specified field. The value is obtained from whichever scan contains the field.
func (ps *ProductScan) GetVal(fldname string) *record.Constant {
	if ps.s1.HasField(fldname) {
		return ps.s1.GetVal(fldname)
	}
	return ps.s2.GetVal(fldname)
}

// HasField returns true if the specified field is in either of the underlying scans.
func (ps *ProductScan) HasField(fldname string) bool {
	return ps.s1.HasField(fldname) || ps.s2.HasField(fldname)
}

// Close closes both underlying scans.
func (ps *ProductScan) Close() {
	ps.s1.Close()
	ps.s2.Close()
}
