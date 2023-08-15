package opt

import (
	"fmt"

	"github.com/kawa1214/simple-db/pkg/db/index"
	"github.com/kawa1214/simple-db/pkg/db/metadata"
	"github.com/kawa1214/simple-db/pkg/db/multibuffer"
	"github.com/kawa1214/simple-db/pkg/db/plan"
	"github.com/kawa1214/simple-db/pkg/db/query"
	"github.com/kawa1214/simple-db/pkg/db/record"
	"github.com/kawa1214/simple-db/pkg/db/tx"
)

// TablePlanner contains methods for planning a single table.
type TablePlanner struct {
	myplan   *plan.TablePlan
	mypred   *query.Predicate
	myschema *record.Schema
	indexes  map[string]*metadata.IndexInfo
	tx       *tx.Transaction
}

// NewTablePlanner creates a new table planner.
func NewTablePlanner(tblname string, mypred *query.Predicate, tx *tx.Transaction, mdm *metadata.MetadataMgr) *TablePlanner {
	myplan := plan.NewTablePlan(tx, tblname, mdm)
	myschema := myplan.Schema()
	indexes := mdm.GetIndexInfo(tblname, tx)
	return &TablePlanner{
		myplan:   myplan,
		mypred:   mypred,
		myschema: myschema,
		indexes:  indexes,
		tx:       tx,
	}
}

// MakeSelectPlan constructs a select plan for the table.
func (tp *TablePlanner) MakeSelectPlan() plan.Plan {
	p := tp.makeIndexSelect()
	if p == nil {
		p = tp.myplan
	}
	return tp.addSelectPred(p)
}

// MakeJoinPlan constructs a join plan of the specified plan and the table.
func (tp *TablePlanner) MakeJoinPlan(current plan.Plan) plan.Plan {
	currsch := current.Schema()
	joinpred := tp.mypred.JoinSubPred(tp.myschema, currsch)
	if joinpred == nil {
		return nil
	}
	p := tp.makeIndexJoin(current, currsch)
	if p == nil {
		p = tp.makeProductJoin(current, currsch)
	}
	return p
}

// MakeProductPlan constructs a product plan of the specified plan and this table.
func (tp *TablePlanner) MakeProductPlan(current plan.Plan) plan.Plan {
	p := tp.addSelectPred(tp.myplan)
	return multibuffer.NewMultibufferProductPlan(tp.tx, current, p)
}

func (tp *TablePlanner) makeIndexSelect() plan.Plan {
	for fldname, ii := range tp.indexes {
		val := tp.mypred.EquatesWithConstant(fldname)
		if val != nil {
			fmt.Println("index on", fldname, "used")
			return index.NewIndexSelectPlan(tp.myplan, ii, val)
		}
	}
	return nil
}

func (tp *TablePlanner) makeIndexJoin(current plan.Plan, currsch *record.Schema) plan.Plan {
	for fldname, ii := range tp.indexes {
		outerfield := tp.mypred.EquatesWithField(fldname)
		if outerfield != "" && currsch.HasField(outerfield) {
			p := index.NewIndexJoinPlan(current, tp.myplan, ii, outerfield)
			addSelectPredPlan := tp.addSelectPred(p)
			return tp.addJoinPred(addSelectPredPlan, currsch)
		}
	}
	return nil
}

func (tp *TablePlanner) makeProductJoin(current plan.Plan, currsch *record.Schema) plan.Plan {
	p := tp.MakeProductPlan(current)
	return tp.addJoinPred(p, currsch)
}

func (tp *TablePlanner) addSelectPred(p plan.Plan) plan.Plan {
	selectpred := tp.mypred.SelectSubPred(tp.myschema)
	if selectpred != nil {
		return plan.NewSelectPlan(p, *selectpred)
	}
	return p
}

func (tp *TablePlanner) addJoinPred(p plan.Plan, currsch *record.Schema) plan.Plan {
	joinpred := tp.mypred.JoinSubPred(currsch, tp.myschema)
	if joinpred != nil {
		return plan.NewSelectPlan(p, *joinpred)
	}
	return p
}
