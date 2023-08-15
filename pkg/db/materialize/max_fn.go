package materialize

import (
	"github.com/kawa1214/simple-db/pkg/db/query"
	"github.com/kawa1214/simple-db/pkg/db/record"
)

// MaxFn is the max aggregation function.
type MaxFn struct {
	fldname string
	val     *record.Constant
}

// NewMaxFn creates a new max aggregation function for the specified field.
func NewMaxFn(fldname string) *MaxFn {
	return &MaxFn{
		fldname: fldname,
	}
}

// ProcessFirst starts a new maximum to be the field value in the current record.
func (m *MaxFn) ProcessFirst(s query.Scan) {
	m.val = s.GetVal(m.fldname)
}

// ProcessNext replaces the current maximum by the field value in the current record, if it is higher.
func (m *MaxFn) ProcessNext(s query.Scan) {
	newval := s.GetVal(m.fldname)
	if newval.CompareTo(m.val) > 0 {
		m.val = newval
	}
}

// FieldName returns the field's name, prepended by "maxof".
func (m *MaxFn) FieldName() string {
	return "maxof" + m.fldname
}

// Value returns the current maximum.
func (m *MaxFn) Value() *record.Constant {
	return m.val
}
