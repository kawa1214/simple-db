package opt

import (
	"github.com/kawa1214/simple-db/pkg/db/metadata"
	"github.com/kawa1214/simple-db/pkg/db/parse"
	"github.com/kawa1214/simple-db/pkg/db/plan"
	"github.com/kawa1214/simple-db/pkg/db/tx"
)

// HeuristicQueryPlanner represents a query planner that optimizes using a heuristic-based algorithm.
type HeuristicQueryPlanner struct {
	tableplanners []TablePlanner
	mdm           *metadata.MetadataMgr
}

// NewHeuristicQueryPlanner creates a new instance of HeuristicQueryPlanner.
func NewHeuristicQueryPlanner(mdm *metadata.MetadataMgr) *HeuristicQueryPlanner {
	return &HeuristicQueryPlanner{
		mdm: mdm,
	}
}

// CreatePlan creates an optimized left-deep query plan using the heuristics.
func (hqp *HeuristicQueryPlanner) CreatePlan(data *parse.QueryData, tx *tx.Transaction) plan.Plan {
	// Step 1: Create a TablePlanner object for each mentioned table
	for _, tblname := range data.Tables() {
		tp := NewTablePlanner(tblname, data.Pred(), tx, hqp.mdm)
		hqp.tableplanners = append(hqp.tableplanners, *tp)
	}

	// Step 2: Choose the lowest-size plan to begin the join order
	currentplan := hqp.getLowestSelectPlan()

	// Step 3: Repeatedly add a plan to the join order
	for len(hqp.tableplanners) > 0 {
		p := hqp.getLowestJoinPlan(currentplan)
		if p != nil {
			currentplan = p
		} else { // no applicable join
			currentplan = hqp.getLowestProductPlan(currentplan)
		}
	}

	// Step 4: Project on the field names and return
	return plan.NewProjectPlan(currentplan, data.Fields())
}

func (hqp *HeuristicQueryPlanner) getLowestSelectPlan() plan.Plan {
	var besttp *TablePlanner
	var bestplan plan.Plan
	for _, tp := range hqp.tableplanners {
		plan := tp.MakeSelectPlan()
		if bestplan == nil || plan.RecordsOutput() < bestplan.RecordsOutput() {
			besttp = &tp
			bestplan = plan
		}
	}
	hqp.removeTablePlanner(besttp)
	return bestplan
}

func (hqp *HeuristicQueryPlanner) getLowestJoinPlan(current plan.Plan) plan.Plan {
	var besttp *TablePlanner
	var bestplan plan.Plan
	for _, tp := range hqp.tableplanners {
		plan := tp.MakeJoinPlan(current)
		if plan != nil && (bestplan == nil || plan.RecordsOutput() < bestplan.RecordsOutput()) {
			besttp = &tp
			bestplan = plan
		}
	}
	if bestplan != nil {
		hqp.removeTablePlanner(besttp)
	}
	return bestplan
}

func (hqp *HeuristicQueryPlanner) getLowestProductPlan(current plan.Plan) plan.Plan {
	var besttp *TablePlanner
	var bestplan plan.Plan
	for _, tp := range hqp.tableplanners {
		plan := tp.MakeProductPlan(current)
		if bestplan == nil || plan.RecordsOutput() < bestplan.RecordsOutput() {
			besttp = &tp
			bestplan = plan
		}
	}
	hqp.removeTablePlanner(besttp)
	return bestplan
}

func (hqp *HeuristicQueryPlanner) removeTablePlanner(tp *TablePlanner) {
	for i, t := range hqp.tableplanners {
		if &t == tp {
			hqp.tableplanners = append(hqp.tableplanners[:i], hqp.tableplanners[i+1:]...)
			break
		}
	}
}

// SetPlanner is for use in planning views, which for simplicity this code doesn't do.
func (hqp *HeuristicQueryPlanner) SetPlanner(p plan.Planner) {
	// Implementation omitted
}
