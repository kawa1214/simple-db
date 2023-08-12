package plan

import (
	"github.com/kawa1214/simple-db/pkg/db/parse"
	"github.com/kawa1214/simple-db/pkg/db/tx"
)

type Planner struct {
	qplanner QueryPlanner
	uplanner UpdatePlanner
}

func NewPlanner(qplanner QueryPlanner, uplanner UpdatePlanner) *Planner {
	return &Planner{
		qplanner: qplanner,
		uplanner: uplanner,
	}
}

func (pl *Planner) CreateQueryPlan(qry string, tx *tx.Transaction) (Plan, error) {
	parser := parse.NewParser(qry)
	data := parser.Query()
	pl.verifyQuery(data)
	return pl.qplanner.CreatePlan(data, tx)
}

func (pl *Planner) ExecuteUpdate(cmd string, tx *tx.Transaction) int {
	parser := parse.NewParser(cmd)
	data := parser.UpdateCmd()
	pl.verifyUpdate(data)

	switch d := data.(type) {
	case *parse.InsertData:
		return pl.uplanner.ExecuteInsert(*d, tx)
	case *parse.DeleteData:
		return pl.uplanner.ExecuteDelete(*d, tx)
	case *parse.ModifyData:
		return pl.uplanner.ExecuteModify(*d, tx)
	case *parse.CreateTableData:
		return pl.uplanner.ExecuteCreateTable(*d, tx)
	case *parse.CreateViewData:
		return pl.uplanner.ExecuteCreateView(*d, tx)
	case *parse.CreateIndexData:
		return pl.uplanner.ExecuteCreateIndex(*d, tx)
	default:
		panic("unknown update type")
	}
}

// SimpleDB does not verify queries, although it should.
func (pl *Planner) verifyQuery(data QueryData) {
}

// SimpleDB does not verify updates, although it should.
func (pl *Planner) verifyUpdate(data interface{}) {
}
