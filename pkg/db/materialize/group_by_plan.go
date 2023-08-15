package materialize

import (
	"github.com/kawa1214/simple-db/pkg/db/plan"
	"github.com/kawa1214/simple-db/pkg/db/query"
	"github.com/kawa1214/simple-db/pkg/db/record"
	"github.com/kawa1214/simple-db/pkg/db/tx"
)

// GroupByPlan implements the plan for the groupby operator.
type GroupByPlan struct {
	p           plan.Plan
	groupfields []string
	aggfns      []AggregationFn
	sch         *record.Schema
}

// NewGroupByPlan creates a groupby plan for the underlying query.
// The grouping is determined by the specified collection of group fields,
// and the aggregation is computed by the specified collection of aggregation functions.
func NewGroupByPlan(tx *tx.Transaction, p plan.Plan, groupfields []string, aggfns []AggregationFn) *GroupByPlan {
	plan := &GroupByPlan{
		p:           NewSortPlan(tx, p, groupfields),
		groupfields: groupfields,
		aggfns:      aggfns,
		sch:         record.NewSchema(),
	}
	for _, fldname := range groupfields {
		plan.sch.Add(fldname, p.Schema())
	}
	for _, fn := range aggfns {
		plan.sch.AddIntField(fn.FieldName())
	}
	return plan
}

// Open opens a sort plan for the specified plan.
// The sort plan ensures that the underlying records will be appropriately grouped.
func (g *GroupByPlan) Open() query.Scan {
	s := g.p.Open()
	return NewGroupByScan(s, g.groupfields, g.aggfns)
}

// BlocksAccessed returns the number of blocks required to compute the aggregation,
// which is one pass through the sorted table.
func (g *GroupByPlan) BlocksAccessed() int {
	return g.p.BlocksAccessed()
}

// RecordsOutput returns the number of groups.
// Assuming equal distribution, this is the product of the distinct values for each grouping field.
func (g *GroupByPlan) RecordsOutput() int {
	numgroups := 1
	for _, fldname := range g.groupfields {
		numgroups *= g.p.DistinctValues(fldname)
	}
	return numgroups
}

// DistinctValues returns the number of distinct values for the specified field.
func (g *GroupByPlan) DistinctValues(fldname string) int {
	if g.p.Schema().HasField(fldname) {
		return g.p.DistinctValues(fldname)
	}
	return g.RecordsOutput()
}

// Schema returns the schema of the output table.
func (g *GroupByPlan) Schema() *record.Schema {
	return g.sch
}
