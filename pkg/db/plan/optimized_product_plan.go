package plan

import (
	"github.com/kawa1214/simple-db/pkg/db/query"
	"github.com/kawa1214/simple-db/pkg/db/record"
)

type OptimizedProductPlan struct {
	bestplan Plan
}

func NewOptimizedProductPlan(p1, p2 Plan) *OptimizedProductPlan {
	prod1 := NewProductPlan(p1, p2)
	prod2 := NewProductPlan(p2, p1)
	b1 := prod1.BlocksAccessed()
	b2 := prod2.BlocksAccessed()

	bestplan := prod1
	if b2 < b1 {
		bestplan = prod2
	}

	return &OptimizedProductPlan{bestplan: bestplan}
}

func (opp *OptimizedProductPlan) Open() query.Scan {
	return opp.bestplan.Open()
}

func (opp *OptimizedProductPlan) BlocksAccessed() int {
	return opp.bestplan.BlocksAccessed()
}

func (opp *OptimizedProductPlan) RecordsOutput() int {
	return opp.bestplan.RecordsOutput()
}

func (opp *OptimizedProductPlan) DistinctValues(fldname string) int {
	return opp.bestplan.DistinctValues(fldname)
}

func (opp *OptimizedProductPlan) Schema() *record.Schema {
	return opp.bestplan.Schema()
}
