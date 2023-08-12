package plan

import (
	"github.com/kawa1214/simple-db/pkg/db/query"
	"github.com/kawa1214/simple-db/pkg/db/record"
)

// SelectPlan is the Plan class corresponding to the select relational algebra operator.
type SelectPlan struct {
	p    Plan
	pred query.Predicate
}

// NewSelectPlan creates a new select node in the query tree, having the specified subquery and predicate.
func NewSelectPlan(p Plan, pred query.Predicate) *SelectPlan {
	return &SelectPlan{
		p:    p,
		pred: pred,
	}
}

// Open creates a select scan for this query.
func (sp *SelectPlan) Open() query.Scan {
	s := sp.p.Open()

	return query.NewSelectScan(s, sp.pred)
}

// BlocksAccessed estimates the number of block accesses in the selection, which is the same as in the underlying query.
func (sp *SelectPlan) BlocksAccessed() int {
	return sp.p.BlocksAccessed()
}

// RecordsOutput estimates the number of output records in the selection, which is determined by the reduction factor of the predicate.
func (sp *SelectPlan) RecordsOutput() int {
	return sp.p.RecordsOutput() / sp.pred.ReductionFactor(sp.p)
}

// DistinctValues estimates the number of distinct field values in the projection.
func (sp *SelectPlan) DistinctValues(fldname string) int {
	if sp.pred.EquatesWithConstant(fldname) != nil {
		return 1
	} else {
		fldname2 := sp.pred.EquatesWithField(fldname)
		if fldname2 != "" {
			return min(sp.p.DistinctValues(fldname), sp.p.DistinctValues(fldname2))
		}
		return sp.p.DistinctValues(fldname)
	}
}

// Schema returns the schema of the selection, which is the same as in the underlying query.
func (sp *SelectPlan) Schema() *record.Schema {
	return sp.p.Schema()
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
