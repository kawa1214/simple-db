package plan

import (
	"errors"

	"github.com/kawa1214/simple-db/pkg/db/metadata"
	"github.com/kawa1214/simple-db/pkg/db/parse"
	"github.com/kawa1214/simple-db/pkg/db/tx"
)

type BetterQueryPlanner struct {
	mdm *metadata.MetadataMgr
}

func NewBetterQueryPlanner(mdm *metadata.MetadataMgr) *BetterQueryPlanner {
	return &BetterQueryPlanner{mdm: mdm}
}

func (bqp *BetterQueryPlanner) CreatePlan(data QueryData, tx *tx.Transaction) (Plan, error) {
	// Step 1: Create a plan for each mentioned table or view.
	var plans []Plan
	for _, tblname := range data.Tables() {
		viewdef := bqp.mdm.GetViewDef(tblname, tx)

		if viewdef != "" {
			parser := parse.NewParser(viewdef)
			viewdata := parser.Query()
			plan, err := bqp.CreatePlan(viewdata, tx)
			if err != nil {
				return nil, err
			}
			plans = append(plans, plan)
		} else {
			plans = append(plans, NewTablePlan(tx, tblname, bqp.mdm))
		}
	}

	// Step 2: Create the product of all table plans
	if len(plans) == 0 {
		return nil, errors.New("no plans available")
	}
	p := plans[0]
	plans = plans[1:]
	for _, nextplan := range plans {
		// Try both orderings and choose the one having lowest cost
		choice1 := NewProductPlan(nextplan, p)
		choice2 := NewProductPlan(p, nextplan)
		if choice1.BlocksAccessed() < choice2.BlocksAccessed() {
			p = choice1
		} else {
			p = choice2
		}
	}

	// Step 3: Add a selection plan for the predicate
	p = NewSelectPlan(p, *data.Pred())

	// Step 4: Project on the field names
	p = NewProjectPlan(p, data.Fields())

	return p, nil
}
