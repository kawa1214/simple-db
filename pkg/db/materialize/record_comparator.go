package materialize

import "github.com/kawa1214/simple-db/pkg/db/query"

// RecordComparator is a comparator for scans.
type RecordComparator struct {
	fields []string
}

// NewRecordComparator creates a comparator using the specified fields.
func NewRecordComparator(fields []string) *RecordComparator {
	return &RecordComparator{fields: fields}
}

// Compare compares the current records of the two specified scans.
// The sort fields are considered in turn.
// When a field is encountered for which the records have
// different values, those values are used as the result
// of the comparison.
// If the two records have the same values for all
// sort fields, then the method returns 0.
func (rc *RecordComparator) Compare(s1, s2 query.Scan) int {
	for _, fldname := range rc.fields {
		val1 := s1.GetVal(fldname)
		val2 := s2.GetVal(fldname)
		result := val1.CompareTo(val2)
		if result != 0 {
			return result
		}
	}
	return 0
}
