package query

import (
	"github.com/kawa1214/simple-db/pkg/db/plan"
	"github.com/kawa1214/simple-db/pkg/db/record"
)

type Term struct {
	lhs, rhs Expression
}

// NewTerm creates a new Term instance with two expressions
func NewTerm(lhs, rhs Expression) *Term {
	return &Term{
		lhs: lhs,
		rhs: rhs,
	}
}

func (t *Term) IsSatisfied(s Scan) bool {
	lhsval := t.lhs.Evaluate(s)
	rhsval := t.rhs.Evaluate(s)
	return lhsval.Equals(rhsval)
}

func (t *Term) ReductionFactor(p plan.Plan) int {
	var lhsName, rhsName string
	if t.lhs.IsFieldName() && t.rhs.IsFieldName() {
		lhsName = t.lhs.AsFieldName()
		rhsName = t.rhs.AsFieldName()
		return max(p.DistinctValues(lhsName), p.DistinctValues(rhsName))
	}
	if t.lhs.IsFieldName() {
		lhsName = t.lhs.AsFieldName()
		return p.DistinctValues(lhsName)
	}
	if t.rhs.IsFieldName() {
		rhsName = t.rhs.AsFieldName()
		return p.DistinctValues(rhsName)
	}
	if t.lhs.AsConstant().Equals(t.rhs.AsConstant()) {
		return 1
	}
	return int(^uint(0) >> 1) // Max int value
}

func (t *Term) EquatesWithConstant(fldname string) *record.Constant {
	if t.lhs.IsFieldName() && t.lhs.AsFieldName() == fldname && !t.rhs.IsFieldName() {
		return t.rhs.AsConstant()
	} else if t.rhs.IsFieldName() && t.rhs.AsFieldName() == fldname && !t.lhs.IsFieldName() {
		return t.lhs.AsConstant()
	}
	return nil
}

func (t *Term) EquatesWithField(fldname string) string {
	if t.lhs.IsFieldName() && t.lhs.AsFieldName() == fldname && t.rhs.IsFieldName() {
		return t.rhs.AsFieldName()
	} else if t.rhs.IsFieldName() && t.rhs.AsFieldName() == fldname && t.lhs.IsFieldName() {
		return t.lhs.AsFieldName()
	}
	return ""
}

func (t *Term) AppliesTo(sch *record.Schema) bool {
	return t.lhs.AppliesTo(sch) && t.rhs.AppliesTo(sch)
}

func (t *Term) String() string {
	return t.lhs.ToString() + "=" + t.rhs.ToString()
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
