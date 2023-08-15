package materialize

import (
	"math"

	"github.com/kawa1214/simple-db/pkg/db/plan"
	"github.com/kawa1214/simple-db/pkg/db/query"
	"github.com/kawa1214/simple-db/pkg/db/record"
	"github.com/kawa1214/simple-db/pkg/db/tx"
)

// MaterializePlan is the plan for the materialize operator.
type MaterializePlan struct {
	srcplan plan.Plan
	tx      *tx.Transaction
}

// NewMaterializePlan creates a new materialize plan for the specified query.
func NewMaterializePlan(tx *tx.Transaction, srcplan plan.Plan) *MaterializePlan {
	return &MaterializePlan{
		srcplan: srcplan,
		tx:      tx,
	}
}

// Open loops through the underlying query, copying its output records into a temporary table.
// It then returns a table scan for that table.
func (mp *MaterializePlan) Open() query.Scan {
	sch := mp.srcplan.Schema()
	temp := NewTempTable(mp.tx, sch)
	src := mp.srcplan.Open()
	dest := temp.Open()
	for src.Next() {
		dest.Insert()
		for _, fldname := range sch.Fields {
			dest.SetVal(fldname, src.GetVal(fldname))
		}
	}
	src.Close()
	dest.BeforeFirst()
	return dest
}

// BlocksAccessed returns the estimated number of blocks in the materialized table.
func (mp *MaterializePlan) BlocksAccessed() int {
	layout := record.NewLayoutFromSchema(mp.srcplan.Schema())
	rpb := float64(mp.tx.BlockSize()) / float64(layout.SlotSize())
	return int(math.Ceil(float64(mp.srcplan.RecordsOutput()) / rpb))
}

// RecordsOutput returns the number of records in the materialized table.
func (mp *MaterializePlan) RecordsOutput() int {
	return mp.srcplan.RecordsOutput()
}

// DistinctValues returns the number of distinct field values.
func (mp *MaterializePlan) DistinctValues(fldname string) int {
	return mp.srcplan.DistinctValues(fldname)
}

// Schema returns the schema of the materialized table.
func (mp *MaterializePlan) Schema() *record.Schema {
	return mp.srcplan.Schema()
}
