package materialize

import (
	"github.com/kawa1214/simple-db/pkg/db/query"
	"github.com/kawa1214/simple-db/pkg/db/record"
)

// GroupValue holds the values of the grouping fields for the current record of a scan.
type GroupValue struct {
	vals map[string]*record.Constant
}

// NewGroupValue creates a new group value, given the specified scan and list of fields.
func NewGroupValue(s query.Scan, fields []string) *GroupValue {
	vals := make(map[string]*record.Constant)
	for _, fldname := range fields {
		vals[fldname] = s.GetVal(fldname)
	}
	return &GroupValue{vals: vals}
}

// GetVal returns the constant value of the specified field in the group.
func (gv *GroupValue) GetVal(fldname string) *record.Constant {
	return gv.vals[fldname]
}

// Equals checks if two GroupValue objects have the same values for their grouping fields.
func (gv *GroupValue) Equals(other *GroupValue) bool {
	for fldname, v1 := range gv.vals {
		v2, ok := other.vals[fldname]
		if !ok || !v1.Equals(v2) {
			return false
		}
	}
	return true
}

// HashCode returns the hash code of a GroupValue object, which is the sum of the hash codes of its field values.
func (gv *GroupValue) HashCode() int {
	hashVal := 0
	for _, c := range gv.vals {
		hashVal += c.HashCode()
	}
	return hashVal
}
